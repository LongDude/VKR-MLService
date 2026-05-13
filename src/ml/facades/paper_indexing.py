from __future__ import annotations

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
from repositories.authors import AuthorRepository
from repositories.institutions import InstitutionRepository
from repositories.paper_meta_sources import PaperMetaSourceRepository
from repositories.papers import PaperRepository
from repositories.taxonomy import TaxonomyRepository
from utils.hashing import calculate_text_hash

from ml.constants import DEFAULT_EMBEDDING_MODEL, PAPERS_COLLECTION
from ml.services.qdrant_payloads import QdrantPayloadBuilder
from ml.services.text_preparation import TextPreparationService


CLUSTER_RECOMPUTE_QUEUE = "queue:cluster_recompute"


class PaperIndexingFacade:
    def __init__(
        self,
        *,
        paper_repository: PaperRepository,
        taxonomy_repository: TaxonomyRepository,
        author_repository: AuthorRepository,
        institution_repository: InstitutionRepository,
        paper_meta_source_repository: PaperMetaSourceRepository,
        embedding_adapter: LMStudioEmbeddingAdapter,
        qdrant_adapter: QdrantAdapter,
        redis_adapter: RedisAdapter,
        text_preparation_service: TextPreparationService | None = None,
        payload_builder: QdrantPayloadBuilder | None = None,
        collection_name: str = PAPERS_COLLECTION,
        embedding_model: str = DEFAULT_EMBEDDING_MODEL,
    ) -> None:
        self.paper_repository = paper_repository
        self.taxonomy_repository = taxonomy_repository
        self.author_repository = author_repository
        self.institution_repository = institution_repository
        self.paper_meta_source_repository = paper_meta_source_repository
        self.embedding_adapter = embedding_adapter
        self.qdrant_adapter = qdrant_adapter
        self.redis_adapter = redis_adapter
        self.text_preparation_service = (
            text_preparation_service or TextPreparationService()
        )
        self.payload_builder = payload_builder or QdrantPayloadBuilder()
        self.collection_name = collection_name
        self.embedding_model = embedding_model

    def index_paper(
        self,
        request: PaperIndexingRequestDTO,
    ) -> PaperIndexingResponseDTO:
        paper = self.paper_repository.get_by_id(request.paper_id)
        if paper is None:
            raise EntityNotFoundError(
                "Paper not found",
                details={"paper_id": request.paper_id},
            )

        title = self._require_title(getattr(paper, "title", None), request.paper_id)
        topics = self.taxonomy_repository.list_topics_by_paper(request.paper_id)
        keywords = self.taxonomy_repository.list_keywords_by_paper(request.paper_id)

        topic_names = [topic.name for topic in topics if getattr(topic, "name", None)]
        keyword_values = [
            keyword.value for keyword in keywords if getattr(keyword, "value", None)
        ]

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
            return PaperIndexingResponseDTO(
                paper_id=request.paper_id,
                status="indexed",
                message="Paper is already indexed; skipped",
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

        authors = self.author_repository.list_by_paper(request.paper_id)
        institution_payload = self._build_institution_payload(authors)
        external_ids = self.paper_meta_source_repository.list_external_ids(
            request.paper_id
        )

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
            external_ids=external_ids,
        )

        self.qdrant_adapter.upsert_point(
            self.collection_name,
            request.paper_id,
            embedding.vector,
            payload,
        )
        self._enqueue_cluster_recompute(
            paper_id=request.paper_id,
            topic_ids=payload.get("topic_ids", []),
            keyword_ids=payload.get("keyword_ids", []),
            text_hash=text_hash,
        )

        return PaperIndexingResponseDTO(
            paper_id=request.paper_id,
            status="indexed",
            message="Paper indexed successfully",
        )

    def index_batch(
        self,
        request: PaperBatchIndexingRequestDTO,
    ) -> BatchOperationResultDTO:
        result = BatchOperationResultDTO(total=len(request.paper_ids))

        for paper_id in request.paper_ids:
            try:
                response = self.index_paper(
                    PaperIndexingRequestDTO(
                        paper_id=paper_id,
                        force_reindex=request.force_reindex,
                    )
                )
            except AppError as exc:
                result.failed += 1
                result.errors.append(
                    {
                        "paper_id": paper_id,
                        "code": exc.code,
                        "message": exc.message,
                        "details": exc.details or {},
                    }
                )
                continue

            if response.message and "skipped" in response.message.lower():
                result.skipped += 1
            else:
                result.updated += 1

        return result

    def _is_current_point_indexed(
        self,
        paper_id: int,
        text_hash: str,
        force_reindex: bool,
    ) -> bool:
        point_exists = self.qdrant_adapter.exists(self.collection_name, paper_id)
        if force_reindex or not point_exists:
            return False

        points = self.qdrant_adapter.retrieve(
            self.collection_name,
            [paper_id],
            with_vectors=False,
        )
        if not points:
            return False
        return points[0].payload.get("text_hash") == text_hash

    def _enqueue_cluster_recompute(
        self,
        *,
        paper_id: int,
        topic_ids: list[int],
        keyword_ids: list[int],
        text_hash: str,
    ) -> None:
        self.redis_adapter.enqueue(
            CLUSTER_RECOMPUTE_QUEUE,
            {
                "task_type": "recompute_topic_clusters",
                "paper_id": paper_id,
                "topic_ids": topic_ids,
                "keyword_ids": keyword_ids,
                "text_hash": text_hash,
            },
        )

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


__all__ = ["PaperIndexingFacade"]
