from __future__ import annotations

import gc
import json
import logging
import math
import re
import string
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

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
NGRAM_RANGE = (1, 4)
SEMANTIC_MODEL_NAME = "sentence-transformers/all-mpnet-base-v2"
SEMANTIC_BATCH_SIZE = 64
SEMANTIC_MAX_SENTENCES = 64
SEMANTIC_SENTENCE_TOP_K = 3
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


class KeywordExtractorAdapter:
    """Load exported ranker weights and score keyphrase candidates for paragraphs."""

    def __init__(
        self,
        model_name: str = DEFAULT_MODEL_NAME,
        top_k: int = DEFAULT_TOP_K,
        artifact_dir: str | Path | None = None,
        filter_similar_candidates: bool = True,
        filter_score_tolerance: float = 0.005,
    ):
        self.default_model_name = str(model_name)
        self.default_top_k = int(top_k)
        self.artifact_root = Path(artifact_dir).resolve() if artifact_dir is not None else Path(__file__).resolve().parent
        self.models_dir = self._resolve_models_dir(self.artifact_root)
        self.manifest_path = self._find_manifest(self.models_dir)
        self.manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.feature_columns = list(self.manifest.get("feature_columns") or FEATURE_COLUMNS)
        self.filter_similar_candidates: bool = filter_similar_candidates
        self.filter_score_tolerance: float = filter_score_tolerance
        self._model_cache: dict[str, Any] = {}
        self._semantic_model: Any | None = None
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
        features = self._build_candidate_features(documents)
        if features.empty:
            return [[] for _ in paragraph_list]

        features = self._add_pke_scores(documents, features)
        features = self._apply_baseline_filter(features)
        features = self._add_semantic_feature(documents, features)
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
            tokens = KeywordExtractorAdapter._ranked_term_tokens(term)
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
            raise RuntimeError("nltk is required for tokenization; install nltk.")
        return _nltk_wordpunct_tokenize(text)

    @staticmethod
    def _normalize_candidate(text: str) -> str:
        if _STEMMER is None:
            raise RuntimeError("nltk is required for Porter stemming; install nltk.")
        tokens = KeywordExtractorAdapter._wordpunct_tokenize(str(text).lower())
        return " ".join(_STEMMER.stem(tok) for tok in tokens)

    def _generate_ngrams(self, tokens: list[str]) -> list[tuple[str, int, int]]:
        min_n, max_n = NGRAM_RANGE
        candidates: list[tuple[str, int, int]] = []
        english_stopwords = self._get_english_stopwords()
        for n in range(min_n, max_n + 1):
            for start in range(0, len(tokens) - n + 1):
                gram_tokens = tokens[start:start + n]
                if all(tok in english_stopwords for tok in gram_tokens):
                    continue
                if not any(tok.isalpha() for tok in gram_tokens):
                    continue
                candidates.append((" ".join(gram_tokens), start, start + n))
        return candidates

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

    def _build_candidate_features(self, documents: pd.DataFrame) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for _, row in documents.iterrows():
            doc_id = row["id"]
            text = str(row["text"])
            if not text.strip():
                continue
            tokens = self._wordpunct_tokenize(text.lower())
            token_count = len(tokens)
            if token_count == 0:
                continue

            pos_tags = self._safe_pos_tags(tokens)
            for candidate, start, end in self._generate_ngrams(tokens):
                candidate_tokens = candidate.split()
                candidate_pos = pos_tags[start:end]
                rows.append({
                    "doc_id": doc_id,
                    "candidate": candidate,
                    "candidate_s": self._normalize_candidate(candidate),
                    "start_token": start,
                    "end_token": end,
                    "first_pos": start / max(token_count, 1),
                    "candidate_len": len(candidate_tokens),
                    "contains_hyphen": int(any("-" in tok for tok in candidate_tokens)),
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

    def _add_pke_scores(self, documents: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
        result = features.copy()
        for spec in self._pke_specs():
            scores_df = self._score_pke_documents(documents, spec)
            result = result.merge(scores_df, on=["doc_id", "candidate_s"], how="left")
            score_col = f"{spec.name}_score"
            rank_col = f"{spec.name}_rank"
            rrank_col = f"{spec.name}_rrank"
            result[score_col] = result[score_col].fillna(0.0)
            result[rank_col] = result[rank_col].fillna(999).astype(int)
            result[rrank_col] = result[rrank_col].fillna(0.0)
        return result

    def _score_pke_documents(self, documents: pd.DataFrame, spec: _PkeSpec) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for _, row in documents.iterrows():
            try:
                keyphrases = self._score_single_pke_document(str(row["text"]), spec)
            except Exception as exc:
                logger.debug("PKE failed doc=%s model=%s error=%s", row["id"], spec.name, exc)
                keyphrases = []

            for rank, (candidate, score) in enumerate(keyphrases, start=1):
                rows.append({
                    "doc_id": row["id"],
                    "candidate_s": self._normalize_candidate(candidate),
                    f"{spec.name}_score": float(score),
                    f"{spec.name}_rank": rank,
                    f"{spec.name}_rrank": 1.0 / rank,
                })

        columns = ["doc_id", "candidate_s", f"{spec.name}_score", f"{spec.name}_rank", f"{spec.name}_rrank"]
        if not rows:
            return pd.DataFrame(columns=columns)
        return (
            pd.DataFrame(rows)
            .sort_values(["doc_id", "candidate_s", f"{spec.name}_score", f"{spec.name}_rank"], ascending=[True, True, False, True])
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

    def _add_semantic_feature(self, documents: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
        result = features.copy()
        result["embedrank_best_sentence_cosine"] = 0.0
        if result.empty:
            return result

        doc_text = documents.set_index("id")["text"].to_dict()
        with self._load_semantic_model() as model:
            for doc_id, group in result.groupby("doc_id", sort=False):
                text = str(doc_text.get(doc_id, ""))
                sentences = self._split_text_into_sentences(text)
                if not sentences or group.empty:
                    continue

                candidates = group["candidate"].astype(str).tolist()
                cand_emb = model.encode(
                    candidates,
                    batch_size=SEMANTIC_BATCH_SIZE,
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                    show_progress_bar=False,
                )
                sent_emb = model.encode(
                    sentences,
                    batch_size=SEMANTIC_BATCH_SIZE,
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                    show_progress_bar=False,
                )
                sim = cand_emb @ sent_emb.T
                result.loc[group.index, "embedrank_best_sentence_cosine"] = sim.max(axis=1).astype(float)
        return result

    @contextmanager
    def _load_semantic_model(self) -> Any:
        if self._semantic_model is not None:
            model = self._semantic_model
        else:
            try:
                from sentence_transformers import SentenceTransformer
            except Exception as exc:
                raise RuntimeError("sentence-transformers is required to compute embedrank_best_sentence_cosine.") from exc
            try:
                model = SentenceTransformer(SEMANTIC_MODEL_NAME)
                self._semantic_model = model
            except Exception as exc:
                raise RuntimeError(f"Failed to load semantic model {SEMANTIC_MODEL_NAME!r}.") from exc
        try:
            yield model
        finally:
            self._release_semantic_model()

    def _release_semantic_model(self) -> None:
        model = self._semantic_model
        self._semantic_model = None
        if model is not None and hasattr(model, "to"):
            try:
                model.to("cpu")
            except Exception:
                logger.debug("Failed to move semantic model to CPU before release.", exc_info=True)
        del model
        gc.collect()
        try:
            import torch

            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except Exception:
            logger.debug("Failed to clear CUDA cache after semantic model release.", exc_info=True)

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
