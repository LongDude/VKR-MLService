from __future__ import annotations

import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pipeline_core.contracts import PipelineContext, PipelineError, StageResult
from pipeline_handlers.base import BasePublishHandler

logger = logging.getLogger(__name__)

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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class KeywordPublishHandler(BasePublishHandler):
    handler_id = "keyword_publish"
    priority = 10

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config=config)
        self.enable_postgres = bool(self.config.get("enable_postgres", True))
        self.postgres_autocreate_publication = bool(self.config.get("postgres_autocreate_publication", True))
        self.enable_qdrant = bool(self.config.get("enable_qdrant", True))
        self.qdrant_collection = str(self.config.get("qdrant_collection", "paper_keywords"))
        self.embedding_model_name = str(
            self.config.get("embedding_model_name", "sentence-transformers/all-MiniLM-L6-v2")
        )
        self.qdrant_similarity_threshold = float(self.config.get("qdrant_similarity_threshold", 0.82))
        self._qdrant_client: Optional[Any] = None
        self._sentence_model: Optional[Any] = None
        self._qdrant_ready = False
        self._qdrant_checked = False

    def can_handle(self, ctx: PipelineContext) -> bool:
        keywords = ctx.runtime_data.get("keywords")
        return isinstance(keywords, list) and len(keywords) > 0

    def recoverable(self, err: Exception) -> bool:
        return False

    @staticmethod
    def _clean_article_id(value: Optional[str]) -> str:
        if not value:
            return ""
        return value.split(":")[-1].strip()

    @staticmethod
    def _split_index(article_id: str) -> Tuple[str, str]:
        candidate = (article_id or "").strip()
        if not candidate:
            return "arXiv", ""
        if ":" in candidate:
            idx_type, idx_value = candidate.split(":", 1)
            idx_type = idx_type.strip() or "arXiv"
            idx_value = idx_value.strip()
            return idx_type, idx_value
        return "arXiv", candidate

    @staticmethod
    def _normalize_date_for_sql(value: Any) -> Optional[str]:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None
        return raw[:10]

    @staticmethod
    def _normalize_keyword(value: str) -> str:
        clean = re.sub(r"[^a-zA-Z0-9]+", " ", value or "")
        return re.sub(r"\s+", " ", clean).strip().lower()

    def _find_publication_id(
        self,
        *,
        conn: Any,
        index_value: str,
        index_type: Optional[str] = None,
    ) -> Optional[int]:
        if not index_value:
            return None
        with conn.cursor() as cursor:
            if index_type:
                cursor.execute(
                    """
                    SELECT ia.fk_publication
                    FROM indexed_at ia
                    JOIN external_indexes ei ON ei.id = ia.fk_external_index
                    WHERE ia.external_index_value = %s
                      AND LOWER(ei.index_name) = LOWER(%s)
                    LIMIT 1
                    """,
                    (index_value, index_type),
                )
            else:
                cursor.execute(
                    """
                    SELECT ia.fk_publication
                    FROM indexed_at ia
                    WHERE ia.external_index_value = %s
                    LIMIT 1
                    """,
                    (index_value,),
                )
            row = cursor.fetchone()
            if row and row[0]:
                return int(row[0])
        return None

    def _resolve_or_create_publication_id(self, ctx: PipelineContext) -> Optional[int]:
        from service_lib import ServiceConnectionFactory

        if ctx.db_id:
            return int(ctx.db_id)

        article_id = str(ctx.article_id or "").strip()
        if not article_id:
            return None

        candidates: List[str] = []
        clean_id = self._clean_article_id(article_id)
        if clean_id:
            candidates.append(clean_id)
        if article_id not in candidates:
            candidates.append(article_id)

        index_type, index_value = self._split_index(article_id)
        metadata = ctx.metadata if isinstance(ctx.metadata, dict) else {}
        title = str(metadata.get("title") or article_id).strip() or article_id
        annotation = metadata.get("abstract") or metadata.get("annotation")
        publication_date = self._normalize_date_for_sql(metadata.get("submitted_date") or metadata.get("date"))

        conn = ServiceConnectionFactory.createDatabaseConnection()
        try:
            for candidate in candidates:
                publication_id = self._find_publication_id(conn=conn, index_value=candidate, index_type=index_type)
                if publication_id is not None:
                    return publication_id

            if not self.postgres_autocreate_publication:
                return None

            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT add_unique_publication(%s, %s, %s, %s, %s)",
                    (title, annotation, publication_date, index_type, index_value),
                )
                row = cursor.fetchone()
                created_id = int(row[0]) if row and row[0] is not None else 0
            conn.commit()

            if created_id > 0:
                logger.info(
                    "PostgreSQL publication created for article_id=%s publication_id=%s",
                    article_id,
                    created_id,
                )
                return created_id

            # add_unique_publication returns 0 when the record already exists.
            publication_id = self._find_publication_id(conn=conn, index_value=index_value, index_type=index_type)
            if publication_id is not None:
                logger.info(
                    "PostgreSQL publication resolved after add_unique_publication returned 0: article_id=%s publication_id=%s",
                    article_id,
                    publication_id,
                )
                return publication_id
            return None
        except Exception:
            conn.rollback()
            logger.exception("Failed to resolve/create publication for article_id=%s", article_id)
            raise
        finally:
            conn.close()

    def _save_postgres_results(
        self, *, publication_id: int, payload: Dict[str, Any], keywords: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        from service_lib import ServiceConnectionFactory

        persist_payload = {
            "article_id": payload.get("article_id"),
            "db_id": publication_id,
            "keyword_count": len(keywords),
            "keywords": keywords,
            "saved_at": _utc_now(),
        }
        serialized = json.dumps(persist_payload, ensure_ascii=False)
        conn = ServiceConnectionFactory.createDatabaseConnection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE publications SET results = %s WHERE id = %s", (serialized, publication_id))
            conn.commit()
            logger.info("PostgreSQL keyword results saved: publication_id=%s", publication_id)
            return {"saved": True, "publication_id": publication_id}
        except Exception:
            conn.rollback()
            logger.exception("Failed to save keyword results to PostgreSQL: publication_id=%s", publication_id)
            raise
        finally:
            conn.close()

    def _build_qdrant_client(self) -> Optional[Any]:
        if QdrantClient is None or qmodels is None:
            logger.warning("qdrant-client is not installed; Qdrant persistence disabled")
            return None
        api_key = os.getenv("QDRANT_API_KEY")
        url = os.getenv("QDRANT_URL")
        host = os.getenv("QDRANT_HOST", "qdrant")
        port = int(os.getenv("QDRANT_PORT", "6333"))
        if url:
            return QdrantClient(url=url, api_key=api_key)
        return QdrantClient(host=host, port=port, api_key=api_key)

    def _ensure_qdrant_ready(self) -> bool:
        if self._qdrant_checked:
            return self._qdrant_ready
        self._qdrant_checked = True

        if not self.enable_qdrant:
            logger.info("Qdrant persistence disabled by config")
            self._qdrant_ready = False
            return False

        if SentenceTransformer is None:
            logger.warning("sentence-transformers is not installed; Qdrant persistence disabled")
            self._qdrant_ready = False
            return False

        try:
            self._qdrant_client = self._build_qdrant_client()
            if not self._qdrant_client:
                self._qdrant_ready = False
                return False
            self._sentence_model = SentenceTransformer(self.embedding_model_name)
            vector_size = self._sentence_model.get_sentence_embedding_dimension()

            collection_exists = False
            try:
                collection_exists = bool(self._qdrant_client.collection_exists(self.qdrant_collection))
            except Exception:
                logger.debug("Qdrant collection_exists not available, fallback to get_collections", exc_info=True)
                collections = self._qdrant_client.get_collections()
                collection_exists = any(col.name == self.qdrant_collection for col in collections.collections)

            if not collection_exists:
                self._qdrant_client.create_collection(
                    collection_name=self.qdrant_collection,
                    vectors_config=qmodels.VectorParams(size=vector_size, distance=qmodels.Distance.COSINE),
                )
                logger.info("Created Qdrant collection: %s", self.qdrant_collection)
            self._qdrant_ready = True
            logger.info("Qdrant persistence initialized: collection=%s", self.qdrant_collection)
            return True
        except Exception:
            logger.exception("Failed to initialize Qdrant persistence")
            self._qdrant_ready = False
            return False

    def _embed(self, value: str) -> Optional[Sequence[float]]:
        if not self._sentence_model:
            return None
        try:
            return self._sentence_model.encode(value).tolist()
        except Exception:
            logger.exception("Embedding failed for keyword: %s", value)
            return None

    def _save_keywords_qdrant(
        self,
        *,
        article_id: str,
        publication_id: Optional[int],
        keywords: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if not self.enable_qdrant:
            return {"saved": False, "reason": "disabled"}
        if not self._ensure_qdrant_ready():
            return {"saved": False, "reason": "not_ready"}
        if not self._qdrant_client or not qmodels:
            return {"saved": False, "reason": "client_missing"}

        points = []
        upserted = 0
        for keyword in keywords:
            canonical = str(keyword.get("canonical") or "").strip()
            normalized = self._normalize_keyword(canonical)
            if not canonical or not normalized:
                continue
            vector = self._embed(canonical)
            if vector is None:
                continue
            point_id = uuid.uuid5(uuid.NAMESPACE_DNS, f"{article_id}:{normalized}").hex
            payload = {
                "canonical": canonical,
                "normalized": normalized,
                "aliases": keyword.get("aliases") or [],
                "sources": keyword.get("sources") or [],
                "relativity": keyword.get("relativity"),
                "article_id": article_id,
                "db_id": publication_id,
                "saved_at": _utc_now(),
            }
            points.append(qmodels.PointStruct(id=point_id, vector=vector, payload=payload))

        if not points:
            return {"saved": False, "reason": "no_points"}

        self._qdrant_client.upsert(collection_name=self.qdrant_collection, points=points)
        upserted = len(points)
        logger.info(
            "Qdrant upsert completed: collection=%s points=%s article_id=%s",
            self.qdrant_collection,
            upserted,
            article_id,
        )
        return {"saved": True, "collection": self.qdrant_collection, "points": upserted}

    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        keywords = ctx.runtime_data.get("keywords")
        if not isinstance(keywords, list) or not keywords:
            return StageResult.fatal_error(
                ctx,
                code="keywords_missing",
                message="Keywords payload is missing",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )

        payload: Dict[str, Any] = {
            "article_id": ctx.article_id,
            "db_id": ctx.db_id,
            "source_type": ctx.source_type,
            "source_ref": ctx.source_ref,
            "keywords": keywords,
            "keyword_count": len(keywords),
            "artifacts": {key: ref.to_dict() for key, ref in ctx.artifacts.items()},
            "trace": [entry.to_dict() for entry in ctx.trace],
            "published_at": _utc_now(),
        }

        persist_status: Dict[str, Any] = {}

        if self.enable_postgres:
            try:
                publication_id = self._resolve_or_create_publication_id(ctx)
                if publication_id is None:
                    logger.warning(
                        "PostgreSQL save skipped: publication_id not resolved for article_id=%s",
                        ctx.article_id,
                    )
                    persist_status["postgres"] = {"saved": False, "reason": "publication_not_found"}
                else:
                    ctx.db_id = publication_id
                    payload["db_id"] = publication_id
                    persist_status["postgres"] = self._save_postgres_results(
                        publication_id=publication_id,
                        payload=payload,
                        keywords=keywords,
                    )
            except Exception as exc:
                return StageResult.fatal_error(
                    ctx,
                    code="postgres_persist_failed",
                    message=str(exc),
                    stage=self.stage.value,
                    handler_id=self.handler_id,
                )
        else:
            persist_status["postgres"] = {"saved": False, "reason": "disabled"}

        try:
            persist_status["qdrant"] = self._save_keywords_qdrant(
                article_id=str(ctx.article_id or ""),
                publication_id=ctx.db_id,
                keywords=keywords,
            )
        except Exception as exc:
            return StageResult.fatal_error(
                ctx,
                code="qdrant_persist_failed",
                message=str(exc),
                stage=self.stage.value,
                handler_id=self.handler_id,
            )

        payload["persisted"] = persist_status
        io.publish_keywords(payload)
        return StageResult.success(ctx)


class ErrorPublishHandler(BasePublishHandler):
    handler_id = "error_publish"
    priority = 1000

    def can_handle(self, ctx: PipelineContext) -> bool:
        return False

    def recoverable(self, err: Exception) -> bool:
        return False

    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        return StageResult.skip(ctx)

    def publish_error(self, ctx: PipelineContext, error: PipelineError, io: Any) -> None:
        payload = {
            "article_id": ctx.article_id,
            "db_id": ctx.db_id,
            "source_type": ctx.source_type,
            "source_ref": ctx.source_ref,
            "error": error.to_dict(),
            "trace": [entry.to_dict() for entry in ctx.trace],
            "failed_at": _utc_now(),
            "profile": ctx.profile,
        }
        io.publish_error(payload)


def get_handlers() -> List[type[BasePublishHandler]]:
    return [KeywordPublishHandler, ErrorPublishHandler]
