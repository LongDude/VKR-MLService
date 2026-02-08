"""Keyword extraction pipeline for parsed scientific articles.

This module defines the ``KeyworkExtractor`` (typo preserved from the
original specification) that consumes parsed article metadata from Redis,
downloads the associated text payload from Minio, extracts scientific
keywords with KeyBERT, SciSpaCy, and YAKE, and synchronizes canonicalized
keywords with a Qdrant vector database.

The extractor focuses on:
* Combining heterogeneous keyword detectors into a single scored list.
* Tracking abbreviations and surface-form aliases so synonymous keywords
  map to the same canonical concept.
* Persisting canonical keyword vectors into Qdrant so the keyword
  inventory naturally expands over time while avoiding duplicates.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from os import getenv
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from minio.error import S3Error

from service_lib import ServiceConnectionFactory

try:  # Optional imports: the pipeline should fail fast only when used.
    from keybert import KeyBERT
except ImportError:  # pragma: no cover - runtime dependency injection
    KeyBERT = None  # type: ignore[assignment]

try:
    import yake
except ImportError:  # pragma: no cover
    yake = None  # type: ignore[assignment]

try:
    import spacy
    from scispacy.abbreviation import AbbreviationDetector
except ImportError:  # pragma: no cover
    spacy = None  # type: ignore[assignment]
    AbbreviationDetector = None  # type: ignore[assignment]

try:
    from sentence_transformers import SentenceTransformer
except ImportError:  # pragma: no cover
    SentenceTransformer = None  # type: ignore[assignment]

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qmodels
except ImportError:  # pragma: no cover
    QdrantClient = None  # type: ignore[assignment]
    qmodels = None  # type: ignore[assignment]

try:
    from langdetect import detect as langdetect_detect
    from langdetect.lang_detect_exception import LangDetectException
except ImportError:  # pragma: no cover
    langdetect_detect = None  # type: ignore[assignment]
    LangDetectException = Exception  # type: ignore[assignment]


logger = logging.getLogger(__name__)


@dataclass
class KeywordVariant:
    """Represents a keyword surface form emitted by an extractor."""

    value: str
    source: str
    score: float
    offsets: Optional[List[Tuple[int, int]]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "source": self.source,
            "score": round(self.score, 6),
            "offsets": self.offsets or [],
        }


@dataclass
class KeywordConcept:
    """Canonical keyword concept composed from several variants."""

    canonical: str
    normalized: str
    variants: List[KeywordVariant] = field(default_factory=list)
    source_scores: Dict[str, float] = field(default_factory=dict)
    combined_score: float = 0.0
    weight_sum: float = 0.0
    occurrences: int = 0
    relativity: float = 0.0
    qdrant_id: Optional[str] = None
    vector: Optional[Sequence[float]] = None

    def add_variant(self, variant: KeywordVariant, weight: float) -> None:
        self.variants.append(variant)
        self.source_scores[variant.source] = max(
            self.source_scores.get(variant.source, 0.0), variant.score
        )
        self.combined_score += variant.score * weight
        self.weight_sum += weight

    def finalize(self, score_weight: float, occurrence_reference: float) -> None:
        if not self.weight_sum:
            base_score = 0.0
        else:
            base_score = max(0.0, min(1.0, self.combined_score / self.weight_sum))

        if occurrence_reference <= 0:
            coverage = 0.0
        else:
            coverage = min(1.0, self.occurrences / occurrence_reference)

        raw_relativity = score_weight * base_score + (1 - score_weight) * coverage
        self.relativity = round(max(0.0, min(1.0, raw_relativity)), 4)

    def to_dict(self) -> Dict[str, Any]:
        aliases = sorted({variant.value for variant in self.variants if variant.value})
        return {
            "canonical": self.canonical,
            "aliases": aliases,
            "source_scores": {k: round(v, 6) for k, v in self.source_scores.items()},
            "relativity": self.relativity,
            "occurrences": self.occurrences,
            "qdrant_id": self.qdrant_id,
            "variants": [variant.to_dict() for variant in self.variants],
        }


class KeyworkExtractor:
    """Extract keywords from parsed articles and persist them into Qdrant."""

    def __init__(
        self,
        redis_postprocess_key: str = "postprocess_queue",
        redis_keywords_key: str = "keyword_queue",
        redis_error_key: str = "keyword_err_queue",
        parsed_text_bucket: str = "parsed-text",
        qdrant_collection: str = "paper_keywords",
        embedding_model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        scispacy_model: str = "en_core_sci_sm",
        keybert_top_n: int = 20,
        yake_top_n: int = 20,
        keybert_diversity: float = 0.5,
        max_text_chars: int = 30000,
        score_weight: float = 0.7,
        occurrence_reference: float = 5.0,
        qdrant_similarity_threshold: float = 0.82,
        language_detection: bool = True,
        default_language: str = "en",
    ) -> None:
        self.redis_postprocess_key = redis_postprocess_key
        self.redis_keywords_key = redis_keywords_key
        self.redis_error_key = redis_error_key
        self.parsed_text_bucket = parsed_text_bucket
        self.qdrant_collection = qdrant_collection
        self.embedding_model_name = embedding_model_name
        self.scispacy_model = scispacy_model
        self.keybert_top_n = keybert_top_n
        self.yake_top_n = yake_top_n
        self.keybert_diversity = keybert_diversity
        self.max_text_chars = max_text_chars
        self.score_weight = min(1.0, max(0.0, score_weight))
        self.occurrence_reference = occurrence_reference
        self.qdrant_similarity_threshold = qdrant_similarity_threshold
        self.language_detection = language_detection
        self.default_language = default_language or "en"

        self.redis_client = ServiceConnectionFactory.getRedisClient()
        self.minio_client = ServiceConnectionFactory.getMinioClient()

        self._method_weights: Dict[str, float] = {
            "keybert": 1.0,
            "yake": 0.8,
            "scispacy": 0.9,
        }

        self._sentence_model = self._load_sentence_model(embedding_model_name)
        self._keybert_model = self._load_keybert_model(self._sentence_model)
        self._yake_extractor = self._load_yake_extractor()
        self._scispacy_model = self._load_scispacy_model(scispacy_model)
        self._ensure_abbreviation_pipe()

        self.qdrant_client = self._build_qdrant_client()
        if self.qdrant_client:
            self._ensure_qdrant_collection()

        self.processed_count = 0

    # ------------------------------------------------------------------
    # Model loaders
    # ------------------------------------------------------------------
    def _load_sentence_model(self, model_name: str) -> Optional[SentenceTransformer]:
        if SentenceTransformer is None:
            logger.warning("sentence-transformers is not installed; embeddings disabled")
            return None
        try:
            return SentenceTransformer(model_name)
        except Exception as exc:  # pragma: no cover - heavy external dependency
            logger.error("Unable to load SentenceTransformer %s: %s", model_name, exc)
            return None

    def _load_keybert_model(
        self, sentence_model: Optional[SentenceTransformer]
    ) -> Optional[KeyBERT]:
        if KeyBERT is None:
            logger.warning("KeyBERT is not installed; disabling KeyBERT extraction")
            return None
        try:
            if sentence_model is not None:
                return KeyBERT(model=sentence_model)
            return KeyBERT()
        except Exception as exc:  # pragma: no cover
            logger.error("Failed to initialize KeyBERT: %s", exc)
            return None

    def _load_yake_extractor(self):
        if yake is None:
            logger.warning("yake is not installed; disabling statistical extraction")
            return None
        try:
            return yake.KeywordExtractor(
                lan="en",
                n=3,
                dedupLim=0.9,
                top=self.yake_top_n,
                features=None,
            )
        except Exception as exc:  # pragma: no cover
            logger.error("Failed to initialize YAKE extractor: %s", exc)
            return None

    def _load_scispacy_model(self, model_name: str):
        if spacy is None:
            logger.warning("spaCy/SciSpaCy is not installed; entity extraction disabled")
            return None
        try:
            return spacy.load(model_name)
        except Exception as exc:  # pragma: no cover
            logger.error("Failed to load SciSpaCy model %s: %s", model_name, exc)
            return None

    def _ensure_abbreviation_pipe(self) -> None:
        if not self._scispacy_model or AbbreviationDetector is None:
            return
        if "abbreviation_detector" in self._scispacy_model.pipe_names:
            return

        try:
            # spaCy 3.7+ registers abbreviation detector by name
            self._scispacy_model.add_pipe("abbreviation_detector")
            return
        except (ValueError, KeyError, AttributeError):
            pass

        try:
            abbreviation_pipe = AbbreviationDetector(self._scispacy_model)
            self._scispacy_model.add_pipe(abbreviation_pipe, name="abbreviation_detector")
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to attach abbreviation detector: %s", exc)

    # ------------------------------------------------------------------
    # Connections
    # ------------------------------------------------------------------
    def _build_qdrant_client(self) -> Optional[QdrantClient]:
        if QdrantClient is None or qmodels is None:
            logger.warning("qdrant-client is not installed; vector sync disabled")
            return None

        api_key = getenv("QDRANT_API_KEY")
        url = getenv("QDRANT_URL")
        host = getenv("QDRANT_HOST", "qdrant")
        port = int(getenv("QDRANT_PORT", "6333"))

        try:
            if url:
                return QdrantClient(url=url, api_key=api_key)
            return QdrantClient(host=host, port=port, api_key=api_key)
        except Exception as exc:  # pragma: no cover
            logger.error("Unable to initialize Qdrant client: %s", exc)
            return None

    def _ensure_qdrant_collection(self) -> None:
        if not self.qdrant_client or not self._sentence_model:
            return

        vector_size = self._sentence_model.get_sentence_embedding_dimension()
        exists = False
        try:
            exists = bool(self.qdrant_client.collection_exists(self.qdrant_collection))
        except AttributeError:
            try:
                collections = self.qdrant_client.get_collections()
                exists = any(col.name == self.qdrant_collection for col in collections.collections)
            except Exception:  # pragma: no cover
                logger.debug("Fallback Qdrant collection listing failed", exc_info=True)
        except Exception:  # pragma: no cover
            logger.debug("Unable to determine Qdrant collection existence", exc_info=True)

        if exists:
            return

        try:
            self.qdrant_client.create_collection(
                collection_name=self.qdrant_collection,
                vectors_config=qmodels.VectorParams(
                    size=vector_size,
                    distance=qmodels.Distance.COSINE,
                ),
            )
            logger.info("Created Qdrant collection %s", self.qdrant_collection)
        except Exception as exc:  # pragma: no cover
            logger.error("Unable to create Qdrant collection %s: %s", self.qdrant_collection, exc)

    # ------------------------------------------------------------------
    # Redis helpers
    # ------------------------------------------------------------------
    def _get_latest_record(self) -> Optional[Dict[str, Any]]:
        try:
            entries = self.redis_client.xrevrange(self.redis_postprocess_key, count=1)
            if not entries:
                return None
            stream_id, payload = entries[0]
            parsed = {key: json.loads(value) for key, value in payload.items()}
            record = parsed.get("data")
            if not isinstance(record, dict):
                return None
            record["stream_id"] = stream_id
            return record
        except Exception as exc:
            logger.error("Failed to read redis stream %s: %s", self.redis_postprocess_key, exc)
            return None

    def _remove_stream_record(self, stream_id: str) -> None:
        if not stream_id:
            return
        try:
            self.redis_client.xdel(self.redis_postprocess_key, stream_id)
        except Exception as exc:
            logger.warning("Unable to remove redis record %s: %s", stream_id, exc)

    def _publish_keywords_record(self, payload: Dict[str, Any]) -> None:
        if not self.redis_keywords_key:
            return
        try:
            self.redis_client.xadd(self.redis_keywords_key, {"data": json.dumps(payload, ensure_ascii=False)})
        except Exception as exc:
            logger.warning("Unable to push keyword summary to %s: %s", self.redis_keywords_key, exc)

    def _move_to_error_queue(self, record: Dict[str, Any], reason: str) -> None:
        if not self.redis_error_key:
            return
        payload = {key: value for key, value in record.items() if key != "stream_id"}
        payload["error"] = reason
        try:
            self.redis_client.xadd(self.redis_error_key, {"data": json.dumps(payload, ensure_ascii=False)})
        except Exception as exc:
            logger.error("Unable to push record to %s: %s", self.redis_error_key, exc)

    # ------------------------------------------------------------------
    # Minio helpers
    # ------------------------------------------------------------------
    def _download_parsed_payload(self, object_name: str) -> Optional[Dict[str, Any]]:
        if not object_name:
            logger.error("Empty text_path received from Redis record")
            return None

        response = None
        try:
            response = self.minio_client.get_object(self.parsed_text_bucket, object_name)
            payload_bytes = response.read()
            return json.loads(payload_bytes.decode("utf-8"))
        except S3Error as exc:
            logger.error("Unable to download parsed text %s: %s", object_name, exc)
        except json.JSONDecodeError as exc:
            logger.error("Parsed text %s is not valid JSON: %s", object_name, exc)
        except Exception as exc:  # pragma: no cover
            logger.error("Unexpected error while downloading parsed text %s: %s", object_name, exc)
        finally:
            if response:
                try:
                    response.close()
                    response.release_conn()
                except Exception:  # pragma: no cover
                    logger.debug("Failed to close Minio response", exc_info=True)
        return None

    def _detect_language(self, text: str) -> str:
        if not text:
            return self.default_language
        if not self.language_detection or langdetect_detect is None:
            return self.default_language
        try:
            detected = langdetect_detect(text)
            if not detected:
                return self.default_language
            return detected.lower()
        except LangDetectException:
            return self.default_language

    # ------------------------------------------------------------------
    # Keyword extraction
    # ------------------------------------------------------------------
    def _extract_with_keybert(self, text: str, language: str) -> List[KeywordVariant]:
        if not text or not self._keybert_model:
            return []
        try:
            stop_words = "english" if language.startswith("en") else None
            candidates = self._keybert_model.extract_keywords(
                text[: self.max_text_chars],
                keyphrase_ngram_range=(1, 3),
                top_n=self.keybert_top_n,
                stop_words=stop_words,
                use_maxsum=True,
                use_mmr=True,
                diversity=self.keybert_diversity,
            )
            return [KeywordVariant(value=kw.strip(), source="keybert", score=float(score)) for kw, score in candidates]
        except Exception as exc:  # pragma: no cover
            logger.warning("KeyBERT extraction failed: %s", exc)
            return []

    def _extract_with_yake(self, text: str) -> List[KeywordVariant]:
        if not text or not self._yake_extractor:
            return []
        try:
            candidates = self._yake_extractor.extract_keywords(text[: self.max_text_chars])
        except Exception as exc:  # pragma: no cover
            logger.warning("YAKE extraction failed: %s", exc)
            return []

        results: List[KeywordVariant] = []
        for phrase, score in candidates:
            if not phrase:
                continue
            normalized_score = 1.0 / (1.0 + float(score))
            results.append(KeywordVariant(value=phrase.strip(), source="yake", score=normalized_score))
        return results

    def _extract_with_scispacy(self, text: str, language: str) -> Tuple[List[KeywordVariant], Dict[str, str]]:
        if not text or not self._scispacy_model:
            return [], {}
        if language and not language.startswith("en"):
            return [], {}

        try:
            doc = self._scispacy_model(text[: self.max_text_chars])
        except Exception as exc:  # pragma: no cover
            logger.warning("SciSpaCy parsing failed: %s", exc)
            return [], {}

        variants: List[KeywordVariant] = []
        synonyms: Dict[str, str] = {}

        preferred_labels = {
            "TASK",
            "METHOD",
            "PROCESS",
            "CHEMICAL",
            "MATERIAL",
            "GENE_OR_GENE_PRODUCT",
            "SCIENTIFIC_TERM",
            "SOFTWARE",
            "EQUIPMENT",
        }

        for ent in doc.ents:
            label = ent.label_.upper()
            if preferred_labels and label not in preferred_labels:
                continue
            score = min(1.0, 0.8 + 0.02 * len(ent.text.split()))
            variants.append(
                KeywordVariant(
                    value=ent.text.strip(),
                    source="scispacy",
                    score=score,
                    offsets=[(ent.start_char, ent.end_char)],
                )
            )

        if hasattr(doc._, "abbreviations") and doc._.abbreviations:
            for abbr in doc._.abbreviations:
                long_form = abbr._.long_form.text.strip()
                short_form = abbr.text.strip()
                synonyms[self._normalize_keyword(short_form)] = long_form

        return variants, synonyms

    def _normalize_keyword(self, value: str) -> str:
        clean = re.sub(r"[^a-zA-Z0-9]+", " ", value or "")
        return re.sub(r"\s+", " ", clean).strip().lower()

    def _estimate_occurrences(self, text: str, concept: KeywordConcept) -> int:
        if not text:
            return 0
        variants = {concept.canonical}
        variants.update({variant.value for variant in concept.variants})
        escaped = [re.escape(v) for v in variants if v]
        if not escaped:
            return 0
        pattern = re.compile(r"\b(" + "|".join(escaped) + r")\b", flags=re.IGNORECASE)
        return len(pattern.findall(text))

    def _aggregate_candidates(
        self,
        text: str,
        variants: Iterable[KeywordVariant],
        synonyms: Dict[str, str],
    ) -> List[KeywordConcept]:
        concepts: Dict[str, KeywordConcept] = {}

        for variant in variants:
            if not variant.value:
                continue
            normalized = self._normalize_keyword(variant.value)
            canonical_value = synonyms.get(normalized, variant.value)
            canonical_normalized = self._normalize_keyword(canonical_value)

            concept = concepts.get(canonical_normalized)
            if not concept:
                concept = KeywordConcept(canonical=canonical_value, normalized=canonical_normalized)
                concepts[canonical_normalized] = concept
            weight = self._method_weights.get(variant.source, 1.0)
            concept.add_variant(variant, weight)

        for concept in concepts.values():
            concept.occurrences = self._estimate_occurrences(text, concept)
            concept.finalize(score_weight=self.score_weight, occurrence_reference=self.occurrence_reference)

        ordered = sorted(concepts.values(), key=lambda item: item.relativity, reverse=True)
        return ordered

    # ------------------------------------------------------------------
    # Qdrant helpers
    # ------------------------------------------------------------------
    def _query_similar_points(self, vector: Sequence[float]):
        if not self.qdrant_client:
            return []

        kwargs = {
            "collection_name": self.qdrant_collection,
            "limit": 1,
            "score_threshold": self.qdrant_similarity_threshold,
            "with_payload": True,
        }

        if hasattr(self.qdrant_client, "search"):
            try:
                result = self.qdrant_client.search(query_vector=vector, **kwargs)
                return result
            except Exception as exc:  # pragma: no cover
                logger.warning("Qdrant search failed: %s", exc)
                return []

        query_kwargs = {"query": vector, **kwargs}

        try:
            if hasattr(self.qdrant_client, "query_points"):
                response = self.qdrant_client.query_points(**query_kwargs)
                return getattr(response, "points", response)
            if hasattr(self.qdrant_client, "query"):
                response = self.qdrant_client.query(**query_kwargs)
                return getattr(response, "points", response)
        except Exception as exc:  # pragma: no cover
            logger.warning("Qdrant query failed: %s", exc)

        logger.warning("Qdrant client version does not expose search/query methods")
        return []

    def _embed(self, value: str) -> Optional[Sequence[float]]:
        if not value or not self._sentence_model:
            return None
        try:
            return self._sentence_model.encode(value).tolist()
        except Exception as exc:  # pragma: no cover
            logger.warning("Embedding failed for '%s': %s", value, exc)
            return None

    def _link_with_qdrant(self, concept: KeywordConcept) -> None:
        if not self.qdrant_client or not self._sentence_model:
            return

        vector = self._embed(concept.canonical)
        if vector is None:
            return
        concept.vector = vector

        search_result = self._query_similar_points(vector)
        if search_result:
            best = search_result[0]
            concept.qdrant_id = str(best.id)
            stored_canonical = best.payload.get("canonical") if isinstance(best.payload, dict) else None
            if stored_canonical:
                concept.canonical = stored_canonical

    def _upsert_into_qdrant(self, article_payload: Dict[str, Any], concepts: List[KeywordConcept]) -> None:
        if not self.qdrant_client or not self._sentence_model or not concepts:
            return

        points: List[Any] = []
        for concept in concepts:
            vector = concept.vector or self._embed(concept.canonical)
            if vector is None:
                continue
            point_id = concept.qdrant_id or uuid.uuid4().hex
            concept.qdrant_id = point_id
            payload = {
                "canonical": concept.canonical,
                "aliases": [variant.value for variant in concept.variants],
                "article_id": article_payload.get("article_id"),
                "db_id": article_payload.get("db_id"),
                "relativity": concept.relativity,
                "source_scores": concept.source_scores,
                "extracted_at": datetime.utcnow().isoformat() + "Z",
            }
            points.append(
                qmodels.PointStruct(
                    id=point_id,
                    vector=vector,
                    payload=payload,
                )
            )

        if not points:
            return

        try:
            self.qdrant_client.upsert(collection_name=self.qdrant_collection, points=points)
        except Exception as exc:  # pragma: no cover
            logger.error("Failed to upsert %d keywords into Qdrant: %s", len(points), exc)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def process_next_record(self) -> bool:
        record = self._get_latest_record()
        if not record:
            return False

        stream_id = record.get("stream_id")
        text_path = record.get("text_path") or record.get("parsed_path")
        parsed_payload = self._download_parsed_payload(text_path)
        if not parsed_payload:
            self._move_to_error_queue(record, "parsed_payload_missing")
            self._remove_stream_record(stream_id)
            return False

        article_payload = {
            "article_id": record.get("article_id") or parsed_payload.get("article_id"),
            "db_id": record.get("db_id") or parsed_payload.get("db_id"),
        }

        text = parsed_payload.get("text") or ""
        if not text.strip():
            self._move_to_error_queue(record, "text_missing")
            self._remove_stream_record(stream_id)
            return False

        language = self._detect_language(text)

        keybert_variants = self._extract_with_keybert(text, language)
        yake_variants = self._extract_with_yake(text)
        scispacy_variants, synonyms = self._extract_with_scispacy(text, language)

        all_variants = list(keybert_variants + yake_variants + scispacy_variants)
        if not all_variants:
            self._move_to_error_queue(record, "no_keywords_extracted")
            self._remove_stream_record(stream_id)
            return False

        concepts = self._aggregate_candidates(text, all_variants, synonyms)
        if not concepts:
            self._move_to_error_queue(record, "aggregation_failed")
            self._remove_stream_record(stream_id)
            return False

        for concept in concepts:
            self._link_with_qdrant(concept)

        self._upsert_into_qdrant(article_payload, concepts)

        summary_payload = {
            **article_payload,
            "text_path": text_path,
            "keyword_count": len(concepts),
            "keywords": [concept.to_dict() for concept in concepts],
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "language": language,
        }
        self._publish_keywords_record(summary_payload)
        self._remove_stream_record(stream_id)
        self.processed_count += 1
        logger.info(
            "Extracted %d keywords for article %s",
            len(concepts),
            article_payload.get("article_id") or "unknown",
        )
        return True

    def run(self, continuous: bool = False) -> None:
        logger.info("KeyworkExtractor started")
        if continuous:
            while self.process_next_record():
                continue
        else:
            self.process_next_record()
        logger.info("KeyworkExtractor stopped after %d articles", self.processed_count)


__all__ = ["KeyworkExtractor", "KeywordVariant", "KeywordConcept"]

if __name__ == "__main__":
    KeyworkExtractor().run()
