from __future__ import annotations

import json
import logging
import re
import string
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from adapters.lmstudio_embedding_adapter import LMStudioEmbeddingAdapter
from core.exceptions import (
    AppError,
    EmbeddingGenerationError,
    EntityNotFoundError,
    InvalidRequestError,
)
from dto.common import BatchOperationResultDTO
from dto.keywords import (
    KeywordExtractionBatchRequestDTO,
    KeywordExtractionItemDTO,
    KeywordExtractionMetadataDTO,
    KeywordExtractionResponseDTO,
    PaperKeywordExtractionBatchRequestDTO,
)
from ml.constants import DEFAULT_EMBEDDING_MODEL
from ml.services.events import EventSink, MLEvent, NoopEventSink
from repositories.papers import PaperRepository

try:
    from nltk.corpus import stopwords as _nltk_stopwords
    from nltk.stem import PorterStemmer as _PorterStemmer
    from nltk.tokenize import wordpunct_tokenize as _nltk_wordpunct_tokenize
except Exception:
    _nltk_stopwords = None
    _PorterStemmer = None
    _nltk_wordpunct_tokenize = None


logger = logging.getLogger(__name__)

DEFAULT_MODEL_NAME = "catboost_lambdamart"
DEFAULT_TOP_K = 10
DEFAULT_EMBEDDING_BATCH_SIZE = 64
DEFAULT_PAPER_EXTRACTION_CHUNK_SIZE = 25
MAX_CANDIDATE_TERMS = 4
SEMANTIC_MAX_SENTENCES = 64
COMPETITION_MAX_CANDIDATES_PER_DOC = 2500

FEATURE_COLUMNS = [
    "contains_hyphen",
    "ends_with_noun",
    "pke_yake_w8_rrank_doc_z",
    "pke_singlerank_w4_rrank_doc_z",
    "pke_rrank_max",
    "pke_rrank_mean",
    "pke_rrank_range",
    "pke_top5_count",
    "pke_top10_count",
    "pke_rrank_delta_yake_singlerank",
    "pke_yake_beats_singlerank",
    "pke_singlerank_beats_yake",
    "embedrank_best_sentence_cosine",
    "candidate_substring_of_other",
    "candidate_contains_other",
    "candidate_token_overlap_max",
]

_STEMMER = _PorterStemmer() if _PorterStemmer is not None else None


@dataclass(frozen=True)
class _PkeSpec:
    name: str
    extractor_name: str
    candidate_selection_kwargs: dict[str, Any]
    candidate_weighting_kwargs: dict[str, Any]
    get_n_best_kwargs: dict[str, Any]


@dataclass(frozen=True)
class _BaselineFilterConfig:
    yake_top_n: int = 150
    singlerank_top_n: int = 150
    agreement_top_n: int = 250
    min_candidates_per_doc: int = 50
    yake_rank_col: str = "pke_yake_w8_rank"
    singlerank_rank_col: str = "pke_singlerank_w4_rank"
    yake_rrank_col: str = "pke_yake_w8_rrank"
    singlerank_rrank_col: str = "pke_singlerank_w4_rrank"


class KeywordExtractionFacade:
    """Extract and persist keyphrases using exported ranker weights."""

    def __init__(
        self,
        model_name: str = DEFAULT_MODEL_NAME,
        top_k: int = DEFAULT_TOP_K,
        artifact_dir: str | Path | None = None,
        filter_similar_candidates: bool = True,
        filter_score_tolerance: float = 0.005,
        paper_repository: PaperRepository | None = None,
        embedding_adapter: LMStudioEmbeddingAdapter | None = None,
        embedding_model: str = DEFAULT_EMBEDDING_MODEL,
        embedding_batch_size: int = DEFAULT_EMBEDDING_BATCH_SIZE,
        paper_extraction_chunk_size: int = DEFAULT_PAPER_EXTRACTION_CHUNK_SIZE,
        event_sink: EventSink | None = None,
    ):
        self.default_model_name = str(model_name)
        self.default_top_k = int(top_k)
        default_artifact_root = Path(__file__).resolve().parents[1] / "model_weights"
        self.artifact_root = Path(artifact_dir).resolve() if artifact_dir is not None else default_artifact_root
        self.models_dir = self._resolve_models_dir(self.artifact_root)
        self.manifest_path = self._find_manifest(self.models_dir)
        self.manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.feature_columns = list(self.manifest.get("feature_columns") or FEATURE_COLUMNS)
        self.filter_similar_candidates: bool = filter_similar_candidates
        self.filter_score_tolerance: float = filter_score_tolerance
        self.paper_repository = paper_repository
        self.embedding_adapter = embedding_adapter or LMStudioEmbeddingAdapter()
        self.embedding_model = embedding_model
        self.embedding_batch_size = max(1, int(embedding_batch_size))
        self.paper_extraction_chunk_size = max(1, int(paper_extraction_chunk_size))
        self.event_sink = event_sink or NoopEventSink()
        self._model_cache: dict[str, Any] = {}
        self._english_stopwords: set[str] | None = None

        self._model_index = {item["name"]: item for item in self.manifest.get("models", [])}
        if self.default_model_name not in self._model_index:
            raise ValueError(f"Unknown model_name={self.default_model_name!r}. Available models: {sorted(self._model_index)}")

    def extract(
        self,
        paragraphs: Sequence[str],
        *,
        model_name: str | None = None,
        top_k: int | None = None,
    ) -> list[list[tuple[str, float]]]:
        if isinstance(paragraphs, str):
            raise TypeError("paragraphs must be a sequence of paragraph strings, not a single string")

        paragraph_list = [str(text) if text is not None else "" for text in paragraphs]
        if not paragraph_list:
            return []

        active_model_name = model_name or self.default_model_name
        limit = self.default_top_k if top_k is None else int(top_k)
        if active_model_name not in self._model_index:
            raise ValueError(f"Unknown model_name={active_model_name!r}. Available models: {sorted(self._model_index)}")

        documents = pd.DataFrame({
            "id": [f"paragraph_{idx}" for idx in range(len(paragraph_list))],
            "text": paragraph_list,
        })
        pke_scores = self._collect_pke_scores(documents)
        features = self._build_candidate_features(documents, pke_scores)
        if features.empty:
            return [[] for _ in paragraph_list]

        features = self._add_pke_scores(features, pke_scores)
        features = self._apply_baseline_filter(features)
        features = self._add_semantic_feature(documents, features, embedding_cache={})
        features = self._postprocess_features(features)
        scores = self._predict(active_model_name, features)
        features = features.copy()
        features["score"] = scores

        outputs: list[list[tuple[str, float]]] = []
        for doc_id in documents["id"].tolist():
            group = features[features["doc_id"] == doc_id].sort_values(
                ["score", "first_pos", "candidate"],
                ascending=[False, True, True],
            )
            ranked_terms = [
                (str(row["candidate"]), float(row["score"]))
                for _, row in group.iterrows()
            ]
            if self.filter_similar_candidates:
                ranked_terms = self.filter_similar_ranked_terms(ranked_terms, min_score_gap=self.filter_score_tolerance)
            if limit >= 0:
                ranked_terms = ranked_terms[:limit]
            outputs.append(ranked_terms)
        return outputs

    def extract_metadata(
        self,
        metadata: KeywordExtractionMetadataDTO,
        *,
        top_k: int | None = None,
        min_score: float | None = None,
    ) -> KeywordExtractionResponseDTO:
        english_metadata = self._resolve_english_metadata(metadata)
        text = self._metadata_text(english_metadata)
        ranked_terms = self.extract([text], top_k=-1)[0]
        ranked_terms = self._apply_output_filters(
            ranked_terms,
            top_k=self.default_top_k if top_k is None else int(top_k),
            min_score=min_score,
        )
        items = [
            KeywordExtractionItemDTO(keyword=keyword, score=float(score))
            for keyword, score in ranked_terms
        ]
        return KeywordExtractionResponseDTO(
            paper_id=metadata.paper_id,
            keywords=[item.keyword for item in items],
            items=items,
        )

    def extract_batch(
        self,
        request: KeywordExtractionBatchRequestDTO,
    ) -> list[KeywordExtractionResponseDTO]:
        if not request.items:
            return []

        source_items = list(request.items)
        resolved_items = [self._resolve_english_metadata(item) for item in source_items]
        texts = [self._metadata_text(item) for item in resolved_items]
        ranked_batches = self.extract(texts, top_k=-1)

        responses: list[KeywordExtractionResponseDTO] = []
        for source, ranked_terms in zip(source_items, ranked_batches):
            filtered_terms = self._apply_output_filters(
                ranked_terms,
                top_k=request.top_k,
                min_score=request.min_score,
            )
            items = [
                KeywordExtractionItemDTO(keyword=keyword, score=float(score))
                for keyword, score in filtered_terms
            ]
            responses.append(
                KeywordExtractionResponseDTO(
                    paper_id=source.paper_id,
                    keywords=[item.keyword for item in items],
                    items=items,
                )
            )
        return responses

    def extract_papers(
        self,
        request: PaperKeywordExtractionBatchRequestDTO,
    ) -> BatchOperationResultDTO:
        if self.paper_repository is None:
            raise InvalidRequestError("PaperRepository is required for paper keyword extraction")
        if request.paper_ids:
            return self._extract_requested_papers(request)
        if (
            request.date_from is None
            and request.date_to is None
            and request.topic_id is None
            and request.field_id is None
        ):
            raise InvalidRequestError(
                "paper_ids, date range, topic_id, or field_id is required for keyword extraction",
            )
        if (
            request.date_from is not None
            and request.date_to is not None
            and request.date_from > request.date_to
        ):
            raise InvalidRequestError(
                "date_from must be less than or equal to date_to",
                details={"date_from": request.date_from, "date_to": request.date_to},
            )
        return self._extract_filtered_papers(request)

    def _extract_requested_papers(
        self,
        request: PaperKeywordExtractionBatchRequestDTO,
    ) -> BatchOperationResultDTO:
        assert self.paper_repository is not None
        requested_ids = list(dict.fromkeys(int(paper_id) for paper_id in request.paper_ids))
        result = BatchOperationResultDTO(total=len(requested_ids))
        papers = self.paper_repository.get_by_ids(requested_ids)
        papers_by_id = {int(paper.id): paper for paper in papers}

        pending_papers: list[Any] = []
        for paper_id in requested_ids:
            paper = papers_by_id.get(paper_id)
            if paper is None:
                result.failed += 1
                result.errors.append(
                    {
                        "paper_id": paper_id,
                        "code": EntityNotFoundError.code,
                        "message": "Paper not found",
                        "details": {"paper_id": paper_id},
                    }
                )
                continue
            if request.skip_processed and getattr(paper, "extracted_keywords", None) is not None:
                result.skipped += 1
                continue
            pending_papers.append(paper)

        self._merge_batch_result(
            result,
            self._extract_loaded_papers(
                pending_papers,
                top_k=request.top_k,
                min_score=request.min_score,
                skip_non_english=request.skip_non_english,
            ),
            keep_total=True,
        )
        return result

    def _extract_filtered_papers(
        self,
        request: PaperKeywordExtractionBatchRequestDTO,
    ) -> BatchOperationResultDTO:
        assert self.paper_repository is not None
        result = BatchOperationResultDTO()
        candidate_ids: list[int] = []
        offset = request.offset
        remaining = request.limit

        while True:
            page_size = request.batch_size
            if remaining is not None:
                page_size = min(page_size, remaining)
            if page_size <= 0:
                break

            page_ids = self.paper_repository.list_ids_for_keyword_extraction(
                date_from=request.date_from,
                date_to=request.date_to,
                topic_id=request.topic_id,
                field_id=request.field_id,
                skip_processed=request.skip_processed,
                limit=page_size,
                offset=offset,
            )
            if not page_ids:
                break
            candidate_ids.extend(page_ids)

            loaded_count = len(page_ids)
            offset += loaded_count
            if remaining is not None:
                remaining -= loaded_count
                if remaining <= 0:
                    break
            if loaded_count < page_size:
                break

        for chunk in self._chunked_ids(candidate_ids, request.batch_size):
            papers = self.paper_repository.get_by_ids(chunk)
            papers_by_id = {int(paper.id): paper for paper in papers}
            ordered_papers = [papers_by_id[paper_id] for paper_id in chunk if paper_id in papers_by_id]
            self._merge_batch_result(
                result,
                self._extract_loaded_papers(
                    ordered_papers,
                    top_k=request.top_k,
                    min_score=request.min_score,
                    skip_non_english=request.skip_non_english,
                ),
            )

        return result

    def _extract_loaded_papers(
        self,
        papers: list[Any],
        *,
        top_k: int,
        min_score: float | None,
        skip_non_english: bool,
    ) -> BatchOperationResultDTO:
        assert self.paper_repository is not None
        result = BatchOperationResultDTO(total=len(papers))
        if not papers:
            return result

        self._emit(
            "keyword_extraction_batch_started",
            entity_id="batch",
            stage="extract",
            current=0,
            total=len(papers),
            message=f"Extracting keywords for {len(papers)} papers",
        )
        pending_papers: list[Any] = []
        pending_metadata: list[KeywordExtractionMetadataDTO] = []
        for paper in papers:
            paper_id = int(paper.id)
            metadata = KeywordExtractionMetadataDTO(
                paper_id=paper_id,
                title=str(getattr(paper, "title", "") or ""),
                abstract=getattr(paper, "abstract", None),
                language=getattr(paper, "language", None),
            )
            try:
                resolved_metadata = self._resolve_english_metadata(metadata)
                self._metadata_text(resolved_metadata)
            except NotImplementedError as exc:
                if skip_non_english:
                    result.skipped += 1
                    self._emit(
                        "keyword_extraction_skipped",
                        entity_id="batch",
                        stage="language",
                        current=result.updated + result.skipped + result.failed,
                        total=len(papers),
                        message=str(exc),
                        payload={"paper_id": paper_id},
                    )
                    continue
                result.failed += 1
                result.errors.append(
                    {
                        "paper_id": paper_id,
                        "code": "not_implemented",
                        "message": str(exc),
                        "details": {"paper_id": paper_id},
                    }
                )
                continue
            except Exception as exc:
                result.failed += 1
                result.errors.append(self._error_payload(exc, paper_id))
                continue

            pending_papers.append(paper)
            pending_metadata.append(resolved_metadata)

        pending_pairs = list(zip(pending_papers, pending_metadata))
        chunk_size = max(
            1,
            int(getattr(self, "paper_extraction_chunk_size", DEFAULT_PAPER_EXTRACTION_CHUNK_SIZE)),
        )
        for chunk in self._chunked_values(pending_pairs, chunk_size):
            chunk_papers = [paper for paper, _ in chunk]
            chunk_metadata = [metadata for _, metadata in chunk]
            try:
                responses = self.extract_batch(
                    KeywordExtractionBatchRequestDTO(
                        items=chunk_metadata,
                        top_k=top_k,
                        min_score=min_score,
                    )
                )
                if len(responses) != len(chunk_metadata):
                    raise ValueError(
                        "Keyword extraction response length does not match chunk size"
                    )
            except Exception as exc:
                for paper in chunk_papers:
                    result.failed += 1
                    result.errors.append(self._error_payload(exc, int(paper.id)))
                self._emit(
                    "keyword_extraction_progress",
                    entity_id="batch",
                    stage="extract",
                    current=result.updated + result.skipped + result.failed,
                    total=len(papers),
                    message=f"Keyword extraction chunk failed: {len(chunk_papers)} papers",
                    payload={"chunk_size": len(chunk_papers)},
                )
                continue
            else:
                extracted_by_id: dict[int, list[str]] = {}
                for paper, response in zip(chunk_papers, responses):
                    paper_id = int(paper.id)
                    extracted_by_id[paper_id] = response.keywords
                    result.updated += 1
                if extracted_by_id:
                    self.paper_repository.save_extracted_keywords(extracted_by_id)
                self._emit(
                    "keyword_extraction_progress",
                    entity_id="batch",
                    stage="extract",
                    current=result.updated + result.skipped + result.failed,
                    total=len(papers),
                    message=f"Extracted keyword chunk: {len(responses)} papers",
                    payload={"chunk_size": len(responses)},
                )

        self._emit(
            "keyword_extraction_batch_completed",
            entity_id="batch",
            stage="completed",
            current=result.updated + result.skipped + result.failed,
            total=result.total,
            message=(
                f"Keyword extraction completed: updated={result.updated} "
                f"skipped={result.skipped} failed={result.failed}"
            ),
            payload=result.model_dump(mode="json"),
        )
        return result

    def _metadata_text(self, metadata: KeywordExtractionMetadataDTO) -> str:
        title = str(metadata.title or "").strip()
        abstract = str(metadata.abstract or "").strip()
        parts = [part for part in (title, abstract) if part]
        if not parts:
            raise InvalidRequestError(
                "Paper title or abstract is required for keyword extraction",
                details={"paper_id": metadata.paper_id},
            )
        return "\n".join(parts)

    def _resolve_english_metadata(
        self,
        metadata: KeywordExtractionMetadataDTO,
    ) -> KeywordExtractionMetadataDTO:
        language = str(metadata.language or "").strip().lower()
        if not language or language in {"en", "eng", "english"}:
            return metadata
        localized = self._find_english_localization_placeholder(metadata)
        if localized is not None:
            return localized
        raise NotImplementedError(
            f"Keyword extraction is not implemented for language={metadata.language!r}"
        )

    def _find_english_localization_placeholder(
        self,
        metadata: KeywordExtractionMetadataDTO,
    ) -> KeywordExtractionMetadataDTO | None:
        _ = metadata
        return None

    def _apply_output_filters(
        self,
        ranked_terms: Sequence[tuple[str, float]],
        *,
        top_k: int,
        min_score: float | None,
    ) -> list[tuple[str, float]]:
        result = [
            (str(keyword), float(score))
            for keyword, score in ranked_terms
            if min_score is None or float(score) >= float(min_score)
        ]
        if top_k >= 0:
            return result[:top_k]
        return result

    def _merge_batch_result(
        self,
        target: BatchOperationResultDTO,
        source: BatchOperationResultDTO,
        *,
        keep_total: bool = False,
    ) -> None:
        if not keep_total:
            target.total += source.total
        target.created += source.created
        target.updated += source.updated
        target.skipped += source.skipped
        target.failed += source.failed
        target.errors.extend(source.errors)

    @staticmethod
    def _chunked_ids(values: list[int], size: int) -> list[list[int]]:
        return [values[index : index + size] for index in range(0, len(values), size)]

    @staticmethod
    def _chunked_values(values: Sequence[Any], size: int) -> list[list[Any]]:
        return [
            list(values[index : index + size])
            for index in range(0, len(values), max(1, int(size)))
        ]

    def _error_payload(self, exc: Exception, paper_id: int) -> dict[str, Any]:
        if isinstance(exc, AppError):
            return {
                "paper_id": paper_id,
                "code": exc.code,
                "message": exc.message,
                "details": exc.details or {},
            }
        return {
            "paper_id": paper_id,
            "code": exc.__class__.__name__,
            "message": str(exc),
            "details": {"paper_id": paper_id},
        }

    @staticmethod
    def _ranked_term_tokens(term: str) -> set[str]:
        return set(re.findall(r"[a-z0-9]+", str(term).lower()))

    @staticmethod
    def filter_similar_ranked_terms(
        ranked_terms: Sequence[tuple[str, float]],
        *,
        overlap_threshold: float = 0.67,
        jaccard_threshold: float = 0.50,
        min_score_gap: float = 0.08,
    ) -> list[tuple[str, float]]:
        if not ranked_terms:
            return []

        scores = [float(score) for _, score in ranked_terms]
        score_scale = max(abs(score) for score in scores) or 1.0
        kept: list[tuple[str, float]] = []
        kept_tokens: list[set[str]] = []
        kept_scores: list[float] = []
        threshold_tolerance = 0.005

        for term, score in ranked_terms:
            score = float(score)
            tokens = KeywordExtractionFacade._ranked_term_tokens(term)
            should_drop = False

            for existing_tokens, existing_score in zip(kept_tokens, kept_scores):
                if tokens == existing_tokens:
                    should_drop = True
                    break
                if not tokens or not existing_tokens:
                    continue

                intersection = len(tokens & existing_tokens)
                if intersection == 0:
                    continue

                containment = intersection / min(len(tokens), len(existing_tokens))
                jaccard = intersection / len(tokens | existing_tokens)
                normalized_score_gap = (existing_score - score) / score_scale
                if (
                    containment + threshold_tolerance >= overlap_threshold
                    and jaccard + threshold_tolerance >= jaccard_threshold
                    and normalized_score_gap >= min_score_gap
                ):
                    should_drop = True
                    break

            if not should_drop:
                kept.append((str(term), score))
                kept_tokens.append(tokens)
                kept_scores.append(score)

        return kept

    @staticmethod
    def _resolve_models_dir(artifact_root: Path) -> Path:
        if (artifact_root / "models").is_dir():
            return artifact_root / "models"
        if artifact_root.is_dir():
            return artifact_root
        raise FileNotFoundError(f"Artifact directory does not exist: {artifact_root}")

    @staticmethod
    def _find_manifest(models_dir: Path) -> Path:
        manifests = sorted(models_dir.glob("booster_weights_manifest_*.json"))
        if not manifests:
            raise FileNotFoundError(f"No booster_weights_manifest_*.json found in {models_dir}")
        return manifests[-1]

    @staticmethod
    def _load_english_stopwords() -> set[str]:
        if _nltk_stopwords is None:
            raise RuntimeError("nltk is required for candidate generation; install nltk and its stopwords corpus.")
        try:
            return set(_nltk_stopwords.words("english"))
        except LookupError as exc:
            raise RuntimeError("NLTK stopwords corpus is required; run nltk.download('stopwords').") from exc

    def _get_english_stopwords(self) -> set[str]:
        if self._english_stopwords is None:
            self._english_stopwords = self._load_english_stopwords()
        return self._english_stopwords

    @staticmethod
    def _wordpunct_tokenize(text: str) -> list[str]:
        if _nltk_wordpunct_tokenize is None:
            return re.findall(r"\w+|[^\w\s]", str(text), flags=re.UNICODE)
        return _nltk_wordpunct_tokenize(text)

    @staticmethod
    def _normalize_candidate(text: str) -> str:
        tokens = KeywordExtractionFacade._candidate_terms(text)
        if _STEMMER is None:
            return " ".join(tokens)
        return " ".join(_STEMMER.stem(tok) for tok in tokens)

    @staticmethod
    def _candidate_terms(text: str) -> list[str]:
        return re.findall(r"[a-z0-9]+(?:-[a-z0-9]+)*", str(text).lower())

    def _clean_candidate(self, text: str) -> str | None:
        terms = self._candidate_terms(text)
        if not self._is_valid_candidate_terms(terms):
            return None
        return " ".join(terms)

    def _is_valid_candidate_terms(self, terms: list[str]) -> bool:
        if not terms or len(terms) > MAX_CANDIDATE_TERMS:
            return False
        if not any(any(ch.isalpha() for ch in term) for term in terms):
            return False

        stopwords = self._get_english_stopwords()
        if all(term in stopwords for term in terms):
            return False
        if terms[0] in stopwords or terms[-1] in stopwords:
            return False
        return True

    @staticmethod
    def _find_term_span(tokens: list[str], terms: list[str]) -> tuple[int, int] | None:
        if not tokens or not terms or len(terms) > len(tokens):
            return None
        width = len(terms)
        for start in range(0, len(tokens) - width + 1):
            if tokens[start : start + width] == terms:
                return start, start + width
        return None

    @staticmethod
    def _simple_pos_tag(token: str) -> str:
        if not any(ch.isalpha() for ch in token):
            return "OTHER"
        if token.endswith(("al", "ic", "ive", "ous", "able", "ible", "ary", "less")):
            return "ADJ"
        return "NOUN"

    @classmethod
    def _safe_pos_tags(cls, tokens: list[str]) -> list[str]:
        try:
            import nltk

            tagged = nltk.pos_tag(tokens)
            coarse = []
            for _, tag in tagged:
                if tag.startswith("NN"):
                    coarse.append("NOUN")
                elif tag.startswith("JJ"):
                    coarse.append("ADJ")
                else:
                    coarse.append("OTHER")
            return coarse
        except Exception:
            return [cls._simple_pos_tag(tok) for tok in tokens]

    def _build_candidate_features(
        self,
        documents: pd.DataFrame,
        pke_scores: dict[str, dict[str, list[tuple[str, float]]]],
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for _, row in documents.iterrows():
            doc_id = str(row["id"])
            text = str(row["text"])
            if not text.strip():
                continue
            tokens = self._candidate_terms(text)
            token_count = len(tokens)
            if token_count == 0:
                continue

            pos_tags = self._safe_pos_tags(tokens)
            seen_candidates: set[str] = set()
            for spec in self._pke_specs():
                for raw_candidate, _ in pke_scores.get(doc_id, {}).get(spec.name, []):
                    candidate = self._clean_candidate(raw_candidate)
                    if candidate is None:
                        continue
                    candidate_s = self._normalize_candidate(candidate)
                    if not candidate_s or candidate_s in seen_candidates:
                        continue
                    seen_candidates.add(candidate_s)

                    candidate_terms = candidate.split()
                    span = self._find_term_span(tokens, candidate_terms)
                    if span is None:
                        start = 0
                        end = len(candidate_terms)
                        candidate_pos = self._safe_pos_tags(candidate_terms)
                    else:
                        start, end = span
                        candidate_pos = pos_tags[start:end]

                    first_pos = start / max(token_count, 1)
                    if span is None:
                        first_pos = 0.0

                    rows.append({
                        "doc_id": doc_id,
                        "candidate": candidate,
                        "candidate_s": candidate_s,
                        "start_token": start,
                        "end_token": end,
                        "first_pos": first_pos,
                        "candidate_len": len(candidate_terms),
                        "contains_hyphen": int(any("-" in tok for tok in candidate_terms)),
                        "ends_with_noun": int(bool(candidate_pos) and candidate_pos[-1] == "NOUN"),
                    })

        if not rows:
            return pd.DataFrame(columns=["doc_id", "candidate", "candidate_s"])

        return (
            pd.DataFrame(rows)
            .sort_values(["doc_id", "candidate_s", "first_pos", "candidate"], ascending=[True, True, True, True])
            .drop_duplicates(["doc_id", "candidate_s"], keep="first")
            .reset_index(drop=True)
        )

    @staticmethod
    def _pke_specs() -> list[_PkeSpec]:
        pos = {"NOUN", "PROPN", "ADJ"}
        return [
            _PkeSpec(
                name="pke_yake_w8",
                extractor_name="YAKE",
                candidate_selection_kwargs={"n": 3},
                candidate_weighting_kwargs={"window": 8, "use_stems": False},
                get_n_best_kwargs={"n": 100, "threshold": 0.8},
            ),
            _PkeSpec(
                name="pke_singlerank_w4",
                extractor_name="SingleRank",
                candidate_selection_kwargs={"pos": pos},
                candidate_weighting_kwargs={"window": 4, "pos": pos, "normalized": False},
                get_n_best_kwargs={"n": 100},
            ),
        ]

    def _add_pke_scores(
        self,
        features: pd.DataFrame,
        pke_scores: dict[str, dict[str, list[tuple[str, float]]]],
    ) -> pd.DataFrame:
        result = features.copy()
        for spec in self._pke_specs():
            scores_df = self._pke_scores_dataframe(pke_scores, spec)
            result = result.merge(scores_df, on=["doc_id", "candidate_s"], how="left")
            score_col = f"{spec.name}_score"
            rank_col = f"{spec.name}_rank"
            rrank_col = f"{spec.name}_rrank"
            result[score_col] = result[score_col].fillna(0.0)
            result[rank_col] = result[rank_col].fillna(999).astype(int)
            result[rrank_col] = result[rrank_col].fillna(0.0)
        return result

    def _collect_pke_scores(
        self,
        documents: pd.DataFrame,
    ) -> dict[str, dict[str, list[tuple[str, float]]]]:
        scores: dict[str, dict[str, list[tuple[str, float]]]] = {}
        for _, row in documents.iterrows():
            doc_id = str(row["id"])
            text = str(row["text"])
            scores.setdefault(doc_id, {})
            if not text.strip():
                continue
            for spec in self._pke_specs():
                try:
                    keyphrases = self._score_single_pke_document(text, spec)
                except Exception as exc:
                    logger.debug("PKE failed doc=%s model=%s error=%s", doc_id, spec.name, exc)
                    keyphrases = []
                scores[doc_id][spec.name] = [
                    (str(candidate), float(score))
                    for candidate, score in keyphrases
                ]
        return scores

    def _pke_scores_dataframe(
        self,
        pke_scores: dict[str, dict[str, list[tuple[str, float]]]],
        spec: _PkeSpec,
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for doc_id, score_by_spec in pke_scores.items():
            keyphrases = score_by_spec.get(spec.name, [])
            for rank, (candidate, score) in enumerate(keyphrases, start=1):
                cleaned_candidate = self._clean_candidate(candidate)
                if cleaned_candidate is None:
                    continue
                rows.append({
                    "doc_id": doc_id,
                    "candidate_s": self._normalize_candidate(cleaned_candidate),
                    f"{spec.name}_score": float(score),
                    f"{spec.name}_rank": rank,
                    f"{spec.name}_rrank": 1.0 / rank,
                })

        columns = ["doc_id", "candidate_s", f"{spec.name}_score", f"{spec.name}_rank", f"{spec.name}_rrank"]
        if not rows:
            return pd.DataFrame(columns=columns)
        return (
            pd.DataFrame(rows)
            .sort_values(["doc_id", "candidate_s", f"{spec.name}_rank"], ascending=[True, True, True])
            .drop_duplicates(["doc_id", "candidate_s"], keep="first")
            .reset_index(drop=True)
        )

    @staticmethod
    def _score_single_pke_document(text: str, spec: _PkeSpec) -> list[tuple[str, float]]:
        try:
            import pke
        except Exception as exc:
            raise RuntimeError("pke is required for YAKE/SingleRank feature extraction.") from exc

        extractor_factory = getattr(pke.unsupervised, spec.extractor_name)
        extractor = extractor_factory()
        extractor.load_document(
            input=text,
            language="en",
            stoplist=list(string.punctuation) + list(pke.lang.stopwords.get("en")),
            normalization=None,
        )
        extractor.candidate_selection(**spec.candidate_selection_kwargs)
        extractor.candidate_weighting(**spec.candidate_weighting_kwargs)
        return extractor.get_n_best(**spec.get_n_best_kwargs)

    def _apply_baseline_filter(self, features: pd.DataFrame) -> pd.DataFrame:
        if features.empty:
            return features

        config = _BaselineFilterConfig()
        result = features.copy().reset_index(drop=True)
        selected_indices: list[int] = []
        for _, group in result.groupby("doc_id", sort=False):
            baseline_columns = {
                config.yake_rank_col,
                config.singlerank_rank_col,
                config.yake_rrank_col,
                config.singlerank_rrank_col,
            }
            if not any(col in group.columns for col in baseline_columns):
                selected_indices.extend(int(idx) for idx in group.index)
                continue

            selected = pd.Index([])
            selected = selected.union(self._top_rank_indices(group, config.yake_rank_col, config.yake_top_n))
            selected = selected.union(self._top_rank_indices(group, config.singlerank_rank_col, config.singlerank_top_n))
            selected = selected.union(self._agreement_indices(group, config))

            min_count = min(config.min_candidates_per_doc, len(group))
            if len(selected) < min_count:
                selected = selected.union(self._supplement_indices(group, min_count - len(selected), config, selected))
            if len(selected) == 0:
                selected = group.index

            selected_set = set(selected)
            selected_indices.extend(int(idx) for idx in group.index if idx in selected_set)

        return result.loc[selected_indices].reset_index(drop=True)

    @staticmethod
    def _numeric_column(group: pd.DataFrame, column: str, default: float) -> pd.Series:
        if column not in group.columns:
            return pd.Series(default, index=group.index, dtype=float)
        return pd.to_numeric(group[column], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default).astype(float)

    def _top_rank_indices(self, group: pd.DataFrame, rank_col: str, top_n: int) -> pd.Index:
        if top_n <= 0 or rank_col not in group.columns:
            return pd.Index([])
        ranks = self._numeric_column(group, rank_col, np.inf)
        return group.index[ranks <= int(top_n)]

    def _agreement_indices(self, group: pd.DataFrame, config: _BaselineFilterConfig) -> pd.Index:
        if config.agreement_top_n <= 0:
            return pd.Index([])
        score_parts = []
        for col in (config.yake_rrank_col, config.singlerank_rrank_col):
            if col in group.columns:
                score_parts.append(self._numeric_column(group, col, 0.0))
        if not score_parts:
            return pd.Index([])
        score = sum(score_parts) / len(score_parts)
        return score.sort_values(ascending=False).head(config.agreement_top_n).index

    def _supplement_indices(
        self,
        group: pd.DataFrame,
        n_needed: int,
        config: _BaselineFilterConfig,
        excluded: pd.Index | None = None,
    ) -> pd.Index:
        if n_needed <= 0:
            return pd.Index([])
        pool = group.drop(index=excluded, errors="ignore") if excluded is not None and len(excluded) else group
        if pool.empty:
            return pd.Index([])
        score_parts = []
        for col in (config.yake_rrank_col, config.singlerank_rrank_col):
            if col in pool.columns:
                score_parts.append(self._numeric_column(pool, col, 0.0))
        if score_parts:
            score = pd.concat(score_parts, axis=1).max(axis=1)
            return score.sort_values(ascending=False).head(n_needed).index
        return pool.sort_values(["first_pos", "candidate_s"], ascending=[True, True]).head(n_needed).index

    def _add_semantic_feature(
        self,
        documents: pd.DataFrame,
        features: pd.DataFrame,
        *,
        embedding_cache: dict[str, list[float]] | None = None,
    ) -> pd.DataFrame:
        result = features.copy()
        result["embedrank_best_sentence_cosine"] = 0.0
        if result.empty:
            return result

        cache = embedding_cache if embedding_cache is not None else {}
        doc_text = documents.set_index("id")["text"].to_dict()
        for doc_id, group in result.groupby("doc_id", sort=False):
            text = str(doc_text.get(doc_id, ""))
            sentences = self._split_text_into_sentences(text)
            if not sentences or group.empty:
                continue

            candidates = group["candidate"].astype(str).tolist()
            cand_emb = self._embed_texts(candidates, embedding_cache=cache)
            sent_emb = self._embed_texts(sentences, embedding_cache=cache)
            if cand_emb.size == 0 or sent_emb.size == 0:
                continue
            sim = cand_emb @ sent_emb.T
            result.loc[group.index, "embedrank_best_sentence_cosine"] = sim.max(axis=1).astype(float)
        return result

    def _embed_texts(
        self,
        texts: Sequence[str],
        *,
        embedding_cache: dict[str, list[float]] | None = None,
    ) -> np.ndarray:
        normalized_texts = [str(text) for text in texts]
        cache = embedding_cache if embedding_cache is not None else {}

        missing_texts: list[str] = []
        seen_missing: set[str] = set()
        for text in normalized_texts:
            if text in cache or text in seen_missing:
                continue
            missing_texts.append(text)
            seen_missing.add(text)

        batch_size = max(1, int(getattr(self, "embedding_batch_size", DEFAULT_EMBEDDING_BATCH_SIZE)))
        for chunk in self._chunked_values(missing_texts, batch_size):
            embeddings = self.embedding_adapter.embed_batch(
                chunk,
                model=self.embedding_model,
            )
            if len(embeddings) != len(chunk):
                raise EmbeddingGenerationError(
                    "Embedding batch response length does not match input length",
                    details={"expected": len(chunk), "actual": len(embeddings)},
                )
            for text, embedding in zip(chunk, embeddings):
                cache[text] = list(embedding.vector or [])

        vectors = [cache.get(text, []) for text in normalized_texts]
        return self._normalize_embedding_matrix(vectors)

    @staticmethod
    def _normalize_embedding_matrix(vectors: Sequence[Sequence[float]]) -> np.ndarray:
        if not vectors:
            return np.zeros((0, 0), dtype=float)
        dimension = max((len(vector) for vector in vectors), default=0)
        if dimension <= 0:
            return np.zeros((len(vectors), 0), dtype=float)

        matrix = np.zeros((len(vectors), dimension), dtype=float)
        for index, vector in enumerate(vectors):
            if not vector:
                continue
            if len(vector) != dimension:
                raise EmbeddingGenerationError(
                    "Embedding vector dimensions do not match",
                    details={"expected": dimension, "actual": len(vector)},
                )
            matrix[index] = np.asarray(vector, dtype=float)

        norms = np.linalg.norm(matrix, axis=1)
        non_zero = norms > 0
        matrix[non_zero] = matrix[non_zero] / norms[non_zero, None]
        return matrix

    @staticmethod
    def _split_text_into_sentences(text: str) -> list[str]:
        text = re.sub(r"\s+", " ", str(text)).strip()
        if not text:
            return []
        sentences = [sentence.strip() for sentence in re.split(r"(?<=[.!?])\s+", text) if sentence.strip()]
        return (sentences or [text])[:SEMANTIC_MAX_SENTENCES]

    def _postprocess_features(self, features: pd.DataFrame) -> pd.DataFrame:
        result = features.copy()
        result = self._add_doc_score_normalization(result)
        result = self._add_pke_agreement_features(result)
        result = self._add_candidate_competition_features(result)
        return self._ensure_feature_columns(result)

    def _add_doc_score_normalization(self, result: pd.DataFrame) -> pd.DataFrame:
        targets = [
            ("pke_yake_w8_rrank", "_doc_z", "pke_yake_w8_rrank_doc_z"),
            ("pke_singlerank_w4_rrank", "_doc_z", "pke_singlerank_w4_rrank_doc_z"),
        ]
        for base_col, suffix, out_col in targets:
            result[out_col] = 0.0
            if base_col not in result.columns:
                continue
            values = result[base_col].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float)
            for _, group in result.groupby("doc_id", sort=False):
                idx = group.index
                x = values.loc[idx]
                if suffix == "_doc_z":
                    mean = float(x.mean())
                    std = float(x.std(ddof=0))
                    result.loc[idx, out_col] = 0.0 if std == 0 else (x - mean) / std
        return result

    @staticmethod
    def _raw_pke_rrank_columns(result: pd.DataFrame) -> list[str]:
        return [
            col for col in result.columns
            if col.startswith("pke_") and col.endswith("_rrank") and not col.endswith("_doc_z")
        ]

    @staticmethod
    def _raw_pke_rank_columns(result: pd.DataFrame) -> list[str]:
        return [
            col for col in result.columns
            if col.startswith("pke_") and col.endswith("_rank") and not col.endswith("_rrank")
        ]

    def _add_pke_agreement_features(self, result: pd.DataFrame) -> pd.DataFrame:
        for col in (
            "pke_rrank_max",
            "pke_rrank_mean",
            "pke_rrank_range",
            "pke_top5_count",
            "pke_top10_count",
            "pke_rrank_delta_yake_singlerank",
            "pke_yake_beats_singlerank",
            "pke_singlerank_beats_yake",
        ):
            result[col] = 0.0

        rrank_cols = self._raw_pke_rrank_columns(result)
        if rrank_cols:
            rrank_frame = result[rrank_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float)
            result["pke_rrank_max"] = rrank_frame.max(axis=1)
            result["pke_rrank_mean"] = rrank_frame.mean(axis=1)
            result["pke_rrank_range"] = rrank_frame.max(axis=1) - rrank_frame.min(axis=1)

        rank_cols = self._raw_pke_rank_columns(result)
        if rank_cols:
            rank_frame = result[rank_cols].replace([np.inf, -np.inf], np.nan).fillna(999).astype(float)
            result["pke_top5_count"] = (rank_frame <= 5).sum(axis=1)
            result["pke_top10_count"] = (rank_frame <= 10).sum(axis=1)

        if {"pke_yake_w8_rrank", "pke_singlerank_w4_rrank"}.issubset(result.columns):
            yake = result["pke_yake_w8_rrank"].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float)
            singlerank = result["pke_singlerank_w4_rrank"].replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(float)
            result["pke_rrank_delta_yake_singlerank"] = yake - singlerank
            result["pke_yake_beats_singlerank"] = (yake > singlerank).astype(int)
            result["pke_singlerank_beats_yake"] = (singlerank > yake).astype(int)
        return result

    def _add_candidate_competition_features(self, result: pd.DataFrame) -> pd.DataFrame:
        for col in ("candidate_substring_of_other", "candidate_contains_other", "candidate_token_overlap_max"):
            result[col] = 0.0

        for _, group in result.groupby("doc_id", sort=False):
            if len(group) > COMPETITION_MAX_CANDIDATES_PER_DOC:
                continue
            idx = group.index
            candidates = group["candidate"].astype(str).str.lower().tolist()
            lengths = group["candidate_len"].astype(int).tolist()
            token_sets = [set(self._wordpunct_tokenize(candidate)) for candidate in candidates]
            substring_of_other = np.zeros(len(group), dtype=int)
            contains_other = np.zeros(len(group), dtype=int)
            overlap_max = np.zeros(len(group), dtype=float)

            for i, candidate_i in enumerate(candidates):
                set_i = token_sets[i]
                for j, candidate_j in enumerate(candidates):
                    if i == j:
                        continue
                    if lengths[j] > lengths[i] and candidate_i and candidate_i in candidate_j:
                        substring_of_other[i] = 1
                    if lengths[j] < lengths[i] and candidate_j and candidate_j in candidate_i:
                        contains_other[i] = 1
                    union = len(set_i | token_sets[j])
                    if union:
                        overlap_max[i] = max(overlap_max[i], len(set_i & token_sets[j]) / union)

            result.loc[idx, "candidate_substring_of_other"] = substring_of_other
            result.loc[idx, "candidate_contains_other"] = contains_other
            result.loc[idx, "candidate_token_overlap_max"] = overlap_max
        return result

    def _ensure_feature_columns(self, features: pd.DataFrame) -> pd.DataFrame:
        result = features.copy()
        for col in self.feature_columns:
            if col not in result.columns:
                result[col] = 0.0
        return result

    def _feature_matrix(self, features: pd.DataFrame) -> pd.DataFrame:
        features = self._ensure_feature_columns(features)
        return features[self.feature_columns].replace([np.inf, -np.inf], 0.0).fillna(0.0)

    def _predict(self, model_name: str, features: pd.DataFrame) -> np.ndarray:
        model = self._load_ranker(model_name)
        X = self._feature_matrix(features)
        return np.asarray(model.predict(X), dtype=float)

    def _load_ranker(self, model_name: str) -> Any:
        if model_name in self._model_cache:
            return self._model_cache[model_name]

        item = self._model_index[model_name]
        model_path = self.models_dir / item["filename"]
        if not model_path.exists():
            raise FileNotFoundError(f"Model weights not found for {model_name}: {model_path}")

        backend = item["backend"]
        if backend == "catboost":
            try:
                from catboost import CatBoostRanker
            except Exception as exc:
                raise RuntimeError("catboost is required to load catboost_lambdamart weights.") from exc
            model = CatBoostRanker()
        elif backend == "xgboost":
            try:
                from xgboost import XGBRanker
            except Exception as exc:
                raise RuntimeError("xgboost is required to load xgb_lambdarank weights.") from exc
            model = XGBRanker()
        else:
            raise ValueError(f"Unsupported backend={backend!r} for model={model_name!r}")

        model.load_model(str(model_path))
        self._model_cache[model_name] = model
        return model

    def _emit(
        self,
        event_type: str,
        *,
        entity_id: str | int | None = None,
        stage: str | None = None,
        current: int | None = None,
        total: int | None = None,
        message: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.event_sink.emit(
            MLEvent(
                event_type=event_type,
                task_type="keyword_extraction",
                entity_id=entity_id,
                stage=stage,
                current=current,
                total=total,
                message=message,
                payload=payload or {},
            )
        )


KeywordExtractorAdapter = KeywordExtractionFacade


__all__ = ["KeywordExtractionFacade", "KeywordExtractorAdapter"]
