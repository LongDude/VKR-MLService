from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from pipeline_core.contracts import ArtifactRef, PipelineContext, StageResult
from pipeline_handlers.base import BaseKeywordHandler

logger = logging.getLogger(__name__)

try:
    from keybert import KeyBERT
except ImportError:  # pragma: no cover
    KeyBERT = None  # type: ignore[assignment]

try:
    import yake
except ImportError:  # pragma: no cover
    yake = None  # type: ignore[assignment]

try:
    import spacy
except ImportError:  # pragma: no cover
    spacy = None  # type: ignore[assignment]


class KeywordEnsembleHandler(BaseKeywordHandler):
    handler_id = "keyword_ensemble"
    priority = 10

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config=config)
        self.max_text_chars = int(self.config.get("max_text_chars", 30000))
        self.max_keywords = int(self.config.get("max_keywords", 25))
        self._weights = {
            "keybert": float(self.config.get("weight_keybert", 1.0)),
            "yake": float(self.config.get("weight_yake", 0.85)),
            "spacy": float(self.config.get("weight_spacy", 0.8)),
            "fallback": 0.5,
        }
        self._keybert = None
        self._yake = None
        self._nlp = None
        self._initialized = False

    def _build_keybert(self):
        if KeyBERT is None:
            return None
        try:
            return KeyBERT()
        except Exception:
            logger.debug("KeyBERT init failed", exc_info=True)
            return None

    def _build_yake(self):
        if yake is None:
            return None
        try:
            return yake.KeywordExtractor(lan="en", n=3, dedupLim=0.9, top=self.max_keywords)
        except Exception:
            logger.debug("YAKE init failed", exc_info=True)
            return None

    def _build_spacy(self):
        if spacy is None:
            return None
        try:
            return spacy.load(self.config.get("spacy_model", "en_core_web_sm"))
        except Exception:
            logger.debug("spaCy init failed", exc_info=True)
            return None

    def can_handle(self, ctx: PipelineContext) -> bool:
        text = ctx.runtime_data.get("clean_text")
        return isinstance(text, str) and bool(text.strip())

    def recoverable(self, err: Exception) -> bool:
        return False

    def _ensure_models(self) -> None:
        if self._initialized:
            return
        self._keybert = self._build_keybert()
        self._yake = self._build_yake()
        self._nlp = self._build_spacy()
        self._initialized = True

    @staticmethod
    def _normalize_keyword(value: str) -> str:
        clean = re.sub(r"[^a-zA-Z0-9]+", " ", value or "")
        return re.sub(r"\s+", " ", clean).strip().lower()

    def _extract_keybert(self, text: str) -> List[Tuple[str, float, str]]:
        if not self._keybert:
            return []
        try:
            pairs = self._keybert.extract_keywords(
                text,
                keyphrase_ngram_range=(1, 3),
                top_n=self.max_keywords,
                use_mmr=True,
                diversity=0.5,
            )
            return [(kw, float(score), "keybert") for kw, score in pairs if kw]
        except Exception:
            logger.debug("KeyBERT extraction failed", exc_info=True)
            return []

    def _extract_yake(self, text: str) -> List[Tuple[str, float, str]]:
        if not self._yake:
            return []
        try:
            pairs = self._yake.extract_keywords(text)
            result: List[Tuple[str, float, str]] = []
            for kw, score in pairs:
                norm = 1.0 / (1.0 + float(score))
                result.append((kw, norm, "yake"))
            return result
        except Exception:
            logger.debug("YAKE extraction failed", exc_info=True)
            return []

    def _extract_spacy(self, text: str) -> List[Tuple[str, float, str]]:
        if not self._nlp:
            return []
        try:
            doc = self._nlp(text)
        except Exception:
            logger.debug("spaCy extraction failed", exc_info=True)
            return []
        items: List[Tuple[str, float, str]] = []
        for ent in doc.ents:
            if ent.text.strip():
                items.append((ent.text.strip(), 0.8, "spacy"))
        return items

    def _extract_fallback(self, text: str) -> List[Tuple[str, float, str]]:
        tokens = re.findall(r"[A-Za-z][A-Za-z0-9\-]{3,}", text.lower())
        counts: Dict[str, int] = defaultdict(int)
        for token in tokens:
            counts[token] += 1
        ordered = sorted(counts.items(), key=lambda item: item[1], reverse=True)
        top = ordered[: self.max_keywords]
        if not top:
            return []
        max_freq = top[0][1]
        return [(kw, freq / max_freq, "fallback") for kw, freq in top if kw]

    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        self._ensure_models()
        text = str(ctx.runtime_data.get("clean_text", ""))[: self.max_text_chars]
        if not text.strip():
            return StageResult.fatal_error(
                ctx,
                code="clean_text_missing",
                message="clean_text is required for keyword extraction",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )

        doc_key = re.sub(r"[^A-Za-z0-9_.-]", "_", ctx.article_id or "article")
        cache_key = f"{doc_key}/keywords.json"
        cached = io.cache.read_bytes("keywords", cache_key)
        if cached is not None:
            keywords_payload = json.loads(cached.decode("utf-8"))
            ctx.runtime_data["keywords"] = keywords_payload
            ctx.register_artifact(
                ArtifactRef(
                    name="keywords",
                    source="cache",
                    content_type="application/json",
                )
            )
            return StageResult.success(ctx)

        candidates: List[Tuple[str, float, str]] = []
        candidates.extend(self._extract_keybert(text))
        candidates.extend(self._extract_yake(text))
        candidates.extend(self._extract_spacy(text))
        if not candidates:
            candidates.extend(self._extract_fallback(text))

        concepts: Dict[str, Dict[str, Any]] = {}
        for value, score, source in candidates:
            normalized = self._normalize_keyword(value)
            if not normalized:
                continue
            concept = concepts.get(normalized)
            weighted = max(0.0, min(1.0, float(score))) * self._weights.get(source, 1.0)
            if concept is None:
                concepts[normalized] = {
                    "canonical": value.strip(),
                    "normalized": normalized,
                    "score_sum": weighted,
                    "score_count": 1,
                    "sources": {source},
                    "aliases": {value.strip()},
                }
            else:
                concept["score_sum"] += weighted
                concept["score_count"] += 1
                concept["sources"].add(source)
                concept["aliases"].add(value.strip())

        keywords = []
        for concept in concepts.values():
            score = concept["score_sum"] / max(1, concept["score_count"])
            keywords.append(
                {
                    "canonical": concept["canonical"],
                    "normalized": concept["normalized"],
                    "relativity": round(max(0.0, min(1.0, score)), 4),
                    "sources": sorted(concept["sources"]),
                    "aliases": sorted(alias for alias in concept["aliases"] if alias),
                }
            )

        keywords.sort(key=lambda item: item["relativity"], reverse=True)
        keywords = keywords[: self.max_keywords]
        if not keywords:
            return StageResult.fatal_error(
                ctx,
                code="no_keywords_extracted",
                message="No keywords extracted from clean text",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )

        io.cache.write_bytes(
            "keywords",
            cache_key,
            json.dumps(keywords, ensure_ascii=False).encode("utf-8"),
            "application/json",
        )
        ctx.runtime_data["keywords"] = keywords
        ctx.register_artifact(
            ArtifactRef(
                name="keywords",
                source="runtime",
                content_type="application/json",
            )
        )
        return StageResult.success(ctx)


def get_handlers() -> List[type[BaseKeywordHandler]]:
    return [KeywordEnsembleHandler]
