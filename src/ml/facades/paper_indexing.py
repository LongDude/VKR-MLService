from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import Any

from adapters.lmstudio_embedding_adapter import LMStudioEmbeddingAdapter
from adapters.qdrant_adapter import QdrantAdapter
from adapters.redis_adapter import RedisAdapter
from core.exceptions import (
    AppError,
    EmbeddingGenerationError,
    EntityNotFoundError,
    InvalidRequestError,
)
from dto.common import BatchOperationResultDTO
from dto.papers import (
    PaperBatchIndexingRequestDTO,
    PaperIndexingRequestDTO,
    PaperIndexingResponseDTO,
)
from dto.qdrant import QdrantPointDTO
from ml.constants import DEFAULT_EMBEDDING_MODEL, PAPERS_COLLECTION
from ml.services.cluster_recompute_tasks import (
    acquire_cluster_recompute_topic_ids,
    build_cluster_recompute_message,
    release_cluster_recompute_dedupe_keys,
)
from ml.services.events import EventSink, MLEvent, NoopEventSink
from ml.services.qdrant_payloads import QdrantPayloadBuilder
from ml.services.text_preparation import TextPreparationService
from repositories.authors import AuthorRepository
from repositories.institutions import InstitutionRepository
from repositories.papers import PaperRepository
from repositories.taxonomy import TaxonomyRepository
from src.dto.enums import IndexingStatus
from utils.hashing import calculate_text_hash

CLUSTER_RECOMPUTE_QUEUE = "queue:cluster_recompute"


class PaperIndexingFacade:
    """Index local papers into the Qdrant content collection.

    The facade coordinates PostgreSQL repositories, LMStudio embeddings, Qdrant
    writes, and Redis follow-up tasks. Single-paper indexing is available for
    CLI/debug flows; batch indexing uses bulk repository reads, batch embedding
    generation, and Qdrant batch upsert.
    """

    def __init__(
        self,
        *,
        paper_repository: PaperRepository,
        taxonomy_repository: TaxonomyRepository,
        author_repository: AuthorRepository,
        institution_repository: InstitutionRepository,
        embedding_adapter: LMStudioEmbeddingAdapter,
        qdrant_adapter: QdrantAdapter,
        redis_adapter: RedisAdapter,
        text_preparation_service: TextPreparationService | None = None,
        payload_builder: QdrantPayloadBuilder | None = None,
        event_sink: EventSink | None = None,
        collection_name: str = PAPERS_COLLECTION,
        embedding_model: str = DEFAULT_EMBEDDING_MODEL,
    ) -> None:
        self.paper_repository = paper_repository
        self.taxonomy_repository = taxonomy_repository
        self.author_repository = author_repository
        self.institution_repository = institution_repository
        self.embedding_adapter = embedding_adapter
        self.qdrant_adapter = qdrant_adapter
        self.redis_adapter = redis_adapter
        self.text_preparation_service = (
            text_preparation_service or TextPreparationService()
        )
        self.payload_builder = payload_builder or QdrantPayloadBuilder()
        self.event_sink = event_sink or NoopEventSink()
        self.collection_name = collection_name
        self.embedding_model = embedding_model

    def index_paper(
        self,
        request: PaperIndexingRequestDTO,
    ) -> PaperIndexingResponseDTO:
        """Index one paper into Qdrant and enqueue cluster recompute."""
        self._emit(
            "paper_indexing_started",
            entity_id=request.paper_id,
            stage="started",
            message="Starting paper indexing",
        )
        paper = self.paper_repository.get_by_id(request.paper_id)
        if paper is None:
            self._emit(
                "paper_indexing_failed",
                entity_id=request.paper_id,
                stage="failed",
                message="Paper not found",
            )
            raise EntityNotFoundError(
                "Paper not found",
                details={"paper_id": request.paper_id},
            )

        qdrant_indexed = False
        try:
            title = self._require_title(getattr(paper, "title", None), request.paper_id)
            topics = self.taxonomy_repository.list_topics_by_paper(request.paper_id)
            keywords = self.taxonomy_repository.list_keywords_by_paper(request.paper_id)

            topic_names = [
                topic.name for topic in topics if getattr(topic, "name", None)
            ]
            keyword_values = [
                keyword.value for keyword in keywords if getattr(keyword, "value", None)
            ]
            keyword_values = self._merge_keyword_values(
                keyword_values,
                getattr(paper, "extracted_keywords", None),
            )

            embedding_text = self.text_preparation_service.build_paper_embedding_text(
                title=title,
                abstract=getattr(paper, "abstract", None),
                topics=topic_names,
                keywords=keyword_values,
            )
            text_hash = calculate_text_hash(embedding_text)

            if self._is_current_point_indexed(
                request.paper_id,
                text_hash,
                request.force_reindex,
            ):
                self._mark_processing_indexed({request.paper_id: text_hash})
                self._emit(
                    "paper_indexing_completed",
                    entity_id=request.paper_id,
                    stage="completed",
                    message="Paper already indexed; skipped",
                    payload={"skipped": True},
                )
                return PaperIndexingResponseDTO(
                    paper_id=request.paper_id,
                    status=IndexingStatus.INDEXED,
                    message="Paper is already indexed; skipped",
                )

            self._mark_processing_started([request.paper_id])
            self._emit(
                "embedding_started",
                entity_id=request.paper_id,
                stage="embedding",
                message="Generating paper embedding",
            )
            embedding = self.embedding_adapter.embed_text(
                embedding_text,
                model=self.embedding_model,
            )
            if embedding is None or not embedding.vector:
                raise EmbeddingGenerationError(
                    "Embedding vector was not generated",
                    details={"paper_id": request.paper_id},
                )
            self._emit(
                "embedding_completed",
                entity_id=request.paper_id,
                stage="embedding",
                message="Paper embedding generated",
                payload={"vector_dimension": len(embedding.vector)},
            )

            authors = self.author_repository.list_by_paper(request.paper_id)
            institution_payload = self._build_institution_payload(authors)

            payload = self.payload_builder.build_paper_payload(
                paper,
                text_hash=text_hash,
                embedding_model=embedding.model or self.embedding_model,
                topic_ids=[
                    topic.id
                    for topic in topics
                    if getattr(topic, "id", None) is not None
                ],
                topic_names=topic_names,
                keyword_ids=[
                    keyword.id
                    for keyword in keywords
                    if getattr(keyword, "id", None) is not None
                ],
                keyword_values=keyword_values,
                author_ids=[
                    author.id
                    for author in authors
                    if getattr(author, "id", None) is not None
                ],
                author_names=[
                    author.display_name
                    for author in authors
                    if getattr(author, "display_name", None)
                ],
                institution_ids=institution_payload["institution_ids"],
                institution_names=institution_payload["institution_names"],
            )

            self.qdrant_adapter.upsert_point(
                self.collection_name,
                request.paper_id,
                embedding.vector,
                payload,
            )
            self._emit(
                "qdrant_upsert_completed",
                entity_id=request.paper_id,
                stage="qdrant_upsert",
                message="Paper point upserted",
                payload={"collection": self.collection_name},
            )
            self._mark_processing_indexed({request.paper_id: text_hash})
            qdrant_indexed = True
            self._enqueue_cluster_recompute(
                topic_ids=payload.get("topic_ids", []),
                workflow_options=self._workflow_options(request),
            )
        except AppError as exc:
            if not qdrant_indexed:
                self._mark_processing_failed([request.paper_id], exc.message)
            self._emit(
                "paper_indexing_failed",
                entity_id=request.paper_id,
                stage="failed",
                message=exc.message,
                payload={"code": exc.code, "details": exc.details or {}},
            )
            raise

        self._emit(
            "paper_indexing_completed",
            entity_id=request.paper_id,
            stage="completed",
            message="Paper indexed successfully",
        )
        return PaperIndexingResponseDTO(
            paper_id=request.paper_id,
            status=IndexingStatus.INDEXED,
            message="Paper indexed successfully",
        )

    def index_batch(
        self,
        request: PaperBatchIndexingRequestDTO,
    ) -> BatchOperationResultDTO:
        """Index papers by explicit ids or by publication-date period.

        Explicit ids are loaded in one repository call and processed through the
        batch path. If ``paper_ids`` is empty, at least one of ``date_from`` or
        ``date_to`` must be provided, and papers are loaded page by page using
        ``batch_size``.
        """
        if request.paper_ids:
            return self._index_requested_ids(request)
        if request.date_from is None and request.date_to is None:
            raise InvalidRequestError(
                "paper_ids or date range is required for batch indexing",
            )
        if (
            request.date_from is not None
            and request.date_to is not None
            and request.date_from > request.date_to
        ):
            raise InvalidRequestError(
                "date_from must be less than or equal to date_to",
                details={
                    "date_from": request.date_from,
                    "date_to": request.date_to,
                },
            )
        return self._index_period_request(request)

    def index_period(
        self,
        date_from: date | None,
        date_to: date | None,
        *,
        force_reindex: bool = False,
        batch_size: int = 200,
        limit: int | None = None,
        offset: int = 0,
    ) -> BatchOperationResultDTO:
        """Index all papers whose publication date falls within a period.

        Papers are loaded from PostgreSQL in pages and each page is processed
        with batch embedding generation and Qdrant batch upsert. The method also
        updates paper processing state, but leaves commit boundaries to callers.
        """
        return self.index_batch(
            PaperBatchIndexingRequestDTO(
                date_from=date_from,
                date_to=date_to,
                force_reindex=force_reindex,
                batch_size=batch_size,
                limit=limit,
                offset=offset,
            )
        )

    def _index_requested_ids(
        self,
        request: PaperBatchIndexingRequestDTO,
    ) -> BatchOperationResultDTO:
        requested_ids = list(dict.fromkeys(request.paper_ids))
        result = BatchOperationResultDTO(total=len(requested_ids))
        papers = self.paper_repository.get_by_ids(requested_ids)
        papers_by_id = {int(paper.id): paper for paper in papers}

        for paper_id in requested_ids:
            if paper_id in papers_by_id:
                continue
            result.failed += 1
            result.errors.append(
                {
                    "paper_id": paper_id,
                    "code": EntityNotFoundError.code,
                    "message": "Paper not found",
                    "details": {"paper_id": paper_id},
                }
            )

        ordered_papers = [
            papers_by_id[paper_id]
            for paper_id in requested_ids
            if paper_id in papers_by_id
        ]
        self._merge_batch_result(
            result,
            self._index_loaded_papers(
                ordered_papers,
                force_reindex=request.force_reindex,
                workflow_options=self._workflow_options(request),
            ),
            keep_total=True,
        )
        return result

    def _index_period_request(
        self,
        request: PaperBatchIndexingRequestDTO,
    ) -> BatchOperationResultDTO:
        result = BatchOperationResultDTO()
        offset = request.offset
        remaining = request.limit

        while True:
            page_size = request.batch_size
            if remaining is not None:
                page_size = min(page_size, remaining)
            if page_size <= 0:
                break

            papers = self.paper_repository.list_by_period(
                request.date_from,
                request.date_to,
                limit=page_size,
                offset=offset,
            )
            if not papers:
                break

            self._merge_batch_result(
                result,
                self._index_loaded_papers(
                    papers,
                    force_reindex=request.force_reindex,
                    workflow_options=self._workflow_options(request),
                ),
            )

            loaded_count = len(papers)
            offset += loaded_count
            if remaining is not None:
                remaining -= loaded_count
                if remaining <= 0:
                    break
            if loaded_count < page_size:
                break

        return result

    def _index_loaded_papers(
        self,
        papers: list[Any],
        *,
        force_reindex: bool,
        workflow_options: dict[str, Any] | None = None,
    ) -> BatchOperationResultDTO:
        result = BatchOperationResultDTO(total=len(papers))
        if not papers:
            return result

        paper_ids = [int(paper.id) for paper in papers]
        self._emit(
            "paper_batch_started",
            entity_id="batch",
            stage="prepare",
            current=0,
            total=len(paper_ids),
            message=f"Preparing {len(paper_ids)} papers",
        )
        topics_by_paper = self.taxonomy_repository.list_topics_by_papers(paper_ids)
        keywords_by_paper = self.taxonomy_repository.list_keywords_by_papers(paper_ids)
        authors_by_paper = self.author_repository.list_by_papers(paper_ids)
        institutions_by_paper = self.institution_repository.list_by_papers(paper_ids)
        existing_hashes = self._existing_text_hashes(paper_ids, force_reindex)

        pending_records: list[dict[str, Any]] = []
        indexed_topic_ids: set[int] = set()
        indexed_text_hashes: dict[int, str] = {}
        skipped_text_hashes: dict[int, str] = {}

        for paper in papers:
            paper_id = int(paper.id)
            try:
                title = self._require_title(getattr(paper, "title", None), paper_id)
            except InvalidRequestError as exc:
                self._mark_processing_failed([paper_id], exc.message)
                result.failed += 1
                result.errors.append(self._error_payload(exc, paper_id))
                continue

            topics = topics_by_paper.get(paper_id, [])
            keywords = keywords_by_paper.get(paper_id, [])
            topic_names = [
                topic.name for topic in topics if getattr(topic, "name", None)
            ]
            keyword_values = [
                keyword.value for keyword in keywords if getattr(keyword, "value", None)
            ]
            keyword_values = self._merge_keyword_values(
                keyword_values,
                getattr(paper, "extracted_keywords", None),
            )
            embedding_text = self.text_preparation_service.build_paper_embedding_text(
                title=title,
                abstract=getattr(paper, "abstract", None),
                topics=topic_names,
                keywords=keyword_values,
            )
            text_hash = calculate_text_hash(embedding_text)

            if not force_reindex and existing_hashes.get(paper_id) == text_hash:
                skipped_text_hashes[paper_id] = text_hash
                result.skipped += 1
                continue

            pending_records.append(
                {
                    "paper": paper,
                    "paper_id": paper_id,
                    "embedding_text": embedding_text,
                    "text_hash": text_hash,
                    "topics": topics,
                    "topic_names": topic_names,
                    "keywords": keywords,
                    "keyword_values": keyword_values,
                    "authors": authors_by_paper.get(paper_id, []),
                    "institutions": institutions_by_paper.get(paper_id, []),
                }
            )

        if not pending_records:
            self._mark_processing_indexed(skipped_text_hashes)
            self._emit(
                "paper_batch_completed",
                entity_id="batch",
                stage="completed",
                current=len(paper_ids),
                total=len(paper_ids),
                message=f"Paper batch skipped={result.skipped} failed={result.failed}",
                payload=result.model_dump(mode="json"),
            )
            return result

        self._mark_processing_started(record["paper_id"] for record in pending_records)
        self._emit(
            "embedding_started",
            entity_id="batch",
            stage="embedding",
            current=0,
            total=len(pending_records),
            message=f"Generating {len(pending_records)} embeddings",
        )
        try:
            embeddings = self.embedding_adapter.embed_batch(
                [record["embedding_text"] for record in pending_records],
                model=self.embedding_model,
            )
        except AppError as exc:
            self._mark_processing_failed(
                (record["paper_id"] for record in pending_records),
                exc.message,
            )
            raise
        self._emit(
            "embedding_completed",
            entity_id="batch",
            stage="embedding",
            current=len(pending_records),
            total=len(pending_records),
            message="Paper embeddings generated",
        )
        if len(embeddings) != len(pending_records):
            self._mark_processing_failed(
                (record["paper_id"] for record in pending_records),
                "Embedding response length does not match batch size",
            )
            raise EmbeddingGenerationError(
                "Embedding response length does not match batch size",
                details={
                    "expected": len(pending_records),
                    "actual": len(embeddings),
                },
            )

        points: list[QdrantPointDTO] = []
        for record, embedding in zip(pending_records, embeddings, strict=True):
            paper_id = record["paper_id"]
            if embedding is None or not embedding.vector:
                self._mark_processing_failed(
                    [paper_id],
                    "Embedding vector was not generated",
                )
                result.failed += 1
                result.errors.append(
                    {
                        "paper_id": paper_id,
                        "code": EmbeddingGenerationError.code,
                        "message": "Embedding vector was not generated",
                        "details": {"paper_id": paper_id},
                    }
                )
                continue

            topic_ids = [
                topic.id
                for topic in record["topics"]
                if getattr(topic, "id", None) is not None
            ]
            keyword_ids = [
                keyword.id
                for keyword in record["keywords"]
                if getattr(keyword, "id", None) is not None
            ]
            indexed_topic_ids.update(int(topic_id) for topic_id in topic_ids)
            indexed_text_hashes[paper_id] = record["text_hash"]

            payload = self.payload_builder.build_paper_payload(
                record["paper"],
                text_hash=record["text_hash"],
                embedding_model=embedding.model or self.embedding_model,
                topic_ids=topic_ids,
                topic_names=record["topic_names"],
                keyword_ids=keyword_ids,
                keyword_values=record["keyword_values"],
                author_ids=[
                    author.id
                    for author in record["authors"]
                    if getattr(author, "id", None) is not None
                ],
                author_names=[
                    author.display_name
                    for author in record["authors"]
                    if getattr(author, "display_name", None)
                ],
                institution_ids=[
                    institution.id
                    for institution in record["institutions"]
                    if getattr(institution, "id", None) is not None
                ],
                institution_names=[
                    institution.display_name
                    for institution in record["institutions"]
                    if getattr(institution, "display_name", None)
                ],
            )
            points.append(
                QdrantPointDTO(
                    id=paper_id,
                    vector=embedding.vector,
                    payload=payload,
                )
            )

        if points:
            try:
                self.qdrant_adapter.upsert_points(self.collection_name, points)
            except AppError as exc:
                self._mark_processing_failed(
                    (int(point.id) for point in points),
                    exc.message,
                )
                raise
            self._emit(
                "qdrant_upsert_completed",
                entity_id="batch",
                stage="qdrant_upsert",
                current=len(points),
                total=len(pending_records),
                message=f"Upserted {len(points)} paper points",
                payload={"collection": self.collection_name},
            )
            self._mark_processing_indexed(indexed_text_hashes)
            result.updated += len(points)
            self._enqueue_cluster_recompute_batch(
                topic_ids=sorted(indexed_topic_ids),
                workflow_options=workflow_options,
            )

        self._mark_processing_indexed(skipped_text_hashes)
        self._emit(
            "paper_batch_completed",
            entity_id="batch",
            stage="completed",
            current=result.updated + result.skipped + result.failed,
            total=result.total,
            message=(
                f"Paper batch completed: updated={result.updated} "
                f"skipped={result.skipped} failed={result.failed}"
            ),
            payload=result.model_dump(mode="json"),
        )
        return result

    def _existing_text_hashes(
        self,
        paper_ids: list[int],
        force_reindex: bool,
    ) -> dict[int, str]:
        if force_reindex or not paper_ids:
            return {}
        result = self.paper_repository.get_indexed_text_hashes(paper_ids)
        missing_ids = [paper_id for paper_id in paper_ids if paper_id not in result]
        if not missing_ids:
            return result

        points = self.qdrant_adapter.retrieve(
            self.collection_name,
            missing_ids,
            with_vectors=False,
        )
        for point in points:
            try:
                paper_id = int(point.id)
            except (TypeError, ValueError):
                continue
            text_hash = point.payload.get("text_hash")
            if text_hash:
                result[paper_id] = str(text_hash)
        return result

    def _is_current_point_indexed(
        self,
        paper_id: int,
        text_hash: str,
        force_reindex: bool,
    ) -> bool:
        if force_reindex:
            return False
        indexed_hashes = self.paper_repository.get_indexed_text_hashes([paper_id])
        if indexed_hashes.get(paper_id) == text_hash:
            return True

        point_exists = self.qdrant_adapter.exists(self.collection_name, paper_id)
        if not point_exists:
            return False

        points = self.qdrant_adapter.retrieve(
            self.collection_name,
            [paper_id],
            with_vectors=False,
        )
        if not points:
            return False
        return points[0].payload.get("text_hash") == text_hash

    def _mark_processing_started(self, paper_ids: Iterable[int]) -> None:
        self.paper_repository.mark_indexing_started(paper_ids)

    def _mark_processing_indexed(self, text_hashes_by_paper_id: dict[int, str]) -> None:
        self.paper_repository.mark_indexed(text_hashes_by_paper_id)

    def _mark_processing_failed(
        self,
        paper_ids: Iterable[int],
        error_message: str,
    ) -> None:
        self.paper_repository.mark_failed(paper_ids, error_message)

    def _enqueue_cluster_recompute(
        self,
        *,
        topic_ids: list[int],
        workflow_options: dict[str, Any] | None = None,
    ) -> None:
        accepted_topic_ids = acquire_cluster_recompute_topic_ids(
            self.redis_adapter,
            topic_ids,
            workflow_options=workflow_options,
        )
        if not accepted_topic_ids:
            return
        message = build_cluster_recompute_message(
            accepted_topic_ids,
            workflow_options=workflow_options,
        )
        try:
            self.redis_adapter.enqueue(CLUSTER_RECOMPUTE_QUEUE, message)
        except Exception:
            release_cluster_recompute_dedupe_keys(self.redis_adapter, message)
            raise

    def _enqueue_cluster_recompute_batch(
        self,
        *,
        topic_ids: list[int],
        workflow_options: dict[str, Any] | None = None,
    ) -> None:
        accepted_topic_ids = acquire_cluster_recompute_topic_ids(
            self.redis_adapter,
            topic_ids,
            workflow_options=workflow_options,
        )
        if not accepted_topic_ids:
            return
        message = build_cluster_recompute_message(
            accepted_topic_ids,
            workflow_options=workflow_options,
        )
        try:
            self.redis_adapter.enqueue(CLUSTER_RECOMPUTE_QUEUE, message)
        except Exception:
            release_cluster_recompute_dedupe_keys(self.redis_adapter, message)
            raise

    def _workflow_options(self, request: Any) -> dict[str, Any]:
        if not getattr(request, "enqueue_cluster_dynamics", False):
            return {}
        date_from = getattr(request, "workflow_date_from", None)
        date_to = getattr(request, "workflow_date_to", None)
        if date_from is None or date_to is None:
            return {}
        source_topic_ids = [
            int(topic_id) for topic_id in getattr(request, "source_topic_ids", []) or []
        ]
        return {
            "source_topic_ids": source_topic_ids,
            "workflow_date_from": date_from.isoformat(),
            "workflow_date_to": date_to.isoformat(),
            "workflow_granularity": getattr(request, "workflow_granularity", "month"),
            "enqueue_cluster_dynamics": True,
        }

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

    def _build_institution_payload(self, authors: list[Any]) -> dict[str, list[Any]]:
        institution_ids: list[int] = []
        institution_names: list[str] = []
        seen_ids: set[int] = set()
        seen_names: set[str] = set()

        for author in authors:
            author_id = getattr(author, "id", None)
            if author_id is None:
                continue
            for institution in self.institution_repository.list_by_author(author_id):
                institution_id = getattr(institution, "id", None)
                if institution_id is not None and institution_id not in seen_ids:
                    seen_ids.add(institution_id)
                    institution_ids.append(institution_id)

                display_name = getattr(institution, "display_name", None)
                if display_name and display_name not in seen_names:
                    seen_names.add(display_name)
                    institution_names.append(display_name)

        return {
            "institution_ids": institution_ids,
            "institution_names": institution_names,
        }

    def _require_title(self, title: str | None, paper_id: int) -> str:
        if title is None or not title.strip():
            raise InvalidRequestError(
                "Paper title is required for indexing",
                details={"paper_id": paper_id},
            )
        return title.strip()

    def _merge_keyword_values(
        self,
        taxonomy_keywords: list[str],
        extracted_keywords: Any,
    ) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for value in [
            *taxonomy_keywords,
            *self._extracted_keyword_values(extracted_keywords),
        ]:
            clean_value = str(value).strip()
            if not clean_value:
                continue
            key = clean_value.lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(clean_value)
        return result

    def _extracted_keyword_values(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        result: list[str] = []
        for item in value:
            if isinstance(item, str):
                result.append(item)
                continue
            if isinstance(item, dict):
                keyword = item.get("keyword") or item.get("value")
                if keyword:
                    result.append(str(keyword))
        return result

    def _error_payload(self, exc: AppError, paper_id: int) -> dict[str, Any]:
        return {
            "paper_id": paper_id,
            "code": exc.code,
            "message": exc.message,
            "details": exc.details or {},
        }

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
                task_type="paper_indexing",
                entity_id=entity_id,
                stage=stage,
                current=current,
                total=total,
                message=message,
                payload=payload or {},
            )
        )


__all__ = ["PaperIndexingFacade"]
