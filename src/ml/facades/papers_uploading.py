from __future__ import annotations

from collections.abc import Callable, Iterable
from decimal import Decimal
from typing import Any, TypeVar

from core.exceptions import AppError, InvalidRequestError
from dto.common import BatchOperationResultDTO, OperationResultDTO
from dto.external import (
    ExternalAuthorDTO,
    ExternalInstitutionDTO,
    ExternalKeywordDTO,
    ExternalLandingDTO,
    ExternalPaperDTO,
    ExternalTopicDTO,
)
from repositories.authors import AuthorRepository
from repositories.institutions import InstitutionRepository
from repositories.landings import LandingRepository
from repositories.paper_meta_sources import PaperMetaSourceRepository
from repositories.papers import PaperRepository
from repositories.taxonomy import TaxonomyRepository


T = TypeVar("T")
R = TypeVar("R")


class PaperUploaderFacade:
    """Cascade-import OpenAlex-like external papers into PostgreSQL repositories.

    The facade expands every ``ExternalPaperDTO`` into atomic entities, deduplicates
    each buffer, upserts parent entities before dependent entities, and then
    restores all paper-author, author-institution, paper-topic, paper-keyword,
    landing, and external-id links. It does not commit the database transaction;
    the caller owns transaction boundaries.
    """

    def __init__(
        self,
        *,
        paper_repository: PaperRepository,
        author_repository: AuthorRepository,
        institution_repository: InstitutionRepository,
        taxonomy_repository: TaxonomyRepository,
        landing_repository: LandingRepository,
        paper_meta_source_repository: PaperMetaSourceRepository,
        paper_batch_size: int = 100,
        author_batch_size: int = 300,
        institution_batch_size: int = 300,
        taxonomy_batch_size: int = 500,
        landing_batch_size: int = 500,
        source_name: str = "openalex",
        source_prefix: str = "https://openalex.org/",
    ) -> None:
        self.paper_repository = paper_repository
        self.author_repository = author_repository
        self.institution_repository = institution_repository
        self.taxonomy_repository = taxonomy_repository
        self.landing_repository = landing_repository
        self.paper_meta_source_repository = paper_meta_source_repository
        self.paper_batch_size = self._validate_batch_size(
            paper_batch_size,
            "paper_batch_size",
        )
        self.author_batch_size = self._validate_batch_size(
            author_batch_size,
            "author_batch_size",
        )
        self.institution_batch_size = self._validate_batch_size(
            institution_batch_size,
            "institution_batch_size",
        )
        self.taxonomy_batch_size = self._validate_batch_size(
            taxonomy_batch_size,
            "taxonomy_batch_size",
        )
        self.landing_batch_size = self._validate_batch_size(
            landing_batch_size,
            "landing_batch_size",
        )
        self.source_name = source_name
        self.source_prefix = source_prefix

    def import_one(
        self,
        paper: ExternalPaperDTO,
        *,
        created_by_user_id: int | None = None,
    ) -> OperationResultDTO:
        """Import one external paper and all nested entities and links."""
        result = self.import_batch(
            [paper],
            created_by_user_id=created_by_user_id,
        )
        return OperationResultDTO(
            success=result.failed == 0,
            message="External paper import completed",
            details=result.model_dump(mode="json"),
        )

    def import_batch(
        self,
        papers: list[ExternalPaperDTO],
        *,
        created_by_user_id: int | None = None,
    ) -> BatchOperationResultDTO:
        """Import external papers in dependency-aware batches.

        Papers are processed in chunks of ``paper_batch_size``. Inside every
        chunk, institutions are upserted before authors, taxonomy parents are
        upserted before topics, papers are upserted before landing/link rows, and
        duplicate external entities are collapsed by stable external keys.
        """
        result = BatchOperationResultDTO(total=len(papers))
        for chunk_index, chunk in enumerate(
            self._chunks(papers, self.paper_batch_size),
        ):
            try:
                imported_count = self._import_chunk(
                    chunk,
                    created_by_user_id=created_by_user_id,
                )
            except AppError as exc:
                result.failed += len(chunk)
                result.errors.append(
                    {
                        "chunk_index": chunk_index,
                        "code": exc.code,
                        "message": exc.message,
                        "details": exc.details or {},
                    }
                )
                continue
            result.updated += imported_count
            result.skipped += len(chunk) - imported_count
        return result

    def _import_chunk(
        self,
        papers: list[ExternalPaperDTO],
        *,
        created_by_user_id: int | None,
    ) -> int:
        buffers = self._build_buffers(papers)

        institutions_by_key = self._upsert_keyed_buffer(
            buffers["institutions"],
            self.institution_repository.upsert_bulk,
            self.institution_batch_size,
        )
        authors_by_key = self._upsert_keyed_buffer(
            buffers["authors"],
            self.author_repository.upsert_bulk,
            self.author_batch_size,
        )
        self._attach_author_institutions(
            buffers["author_institution_links"],
            authors_by_key,
            institutions_by_key,
        )

        topics_by_key = self._upsert_keyed_buffer(
            buffers["topics"],
            self.taxonomy_repository.upsert_bulk,
            self.taxonomy_batch_size,
        )
        keywords_by_key = self._upsert_keyed_buffer(
            buffers["keywords"],
            self.taxonomy_repository.upsert_bulk,
            self.taxonomy_batch_size,
        )

        papers_by_key = self._upsert_keyed_buffer(
            buffers["papers"],
            lambda items: self.paper_repository.upsert_bulk(
                items,
                created_by_user_id=created_by_user_id,
                source_name=self.source_name,
            ),
            self.paper_batch_size,
        )
        self._attach_paper_external_ids(
            buffers["paper_external_ids"],
            papers_by_key,
        )
        self._upsert_landings(buffers["landings"], papers_by_key)
        self._attach_paper_authors(
            buffers["paper_author_links"],
            papers_by_key,
            authors_by_key,
        )
        self._attach_paper_topics(
            buffers["paper_topic_links"],
            papers_by_key,
            topics_by_key,
        )
        self._attach_paper_keywords(
            buffers["paper_keyword_links"],
            papers_by_key,
            keywords_by_key,
        )
        self.paper_repository.session.flush()
        return len(buffers["papers"])

    def _build_buffers(self, papers: Iterable[ExternalPaperDTO]) -> dict[str, Any]:
        buffers: dict[str, Any] = {
            "papers": {},
            "authors": {},
            "institutions": {},
            "topics": {},
            "keywords": {},
            "landings": {},
            "paper_external_ids": {},
            "author_institution_links": set(),
            "paper_author_links": {},
            "paper_topic_links": {},
            "paper_keyword_links": {},
        }

        for paper in papers:
            
            # Patch to catch empty titles
            if paper.title is None or paper.title == "":
                continue

            paper_key = self._paper_key(paper)
            buffers["papers"][paper_key] = paper
            if paper.external_id:
                buffers["paper_external_ids"][paper_key] = paper.external_id

            for institution in paper.institutions:
                institution_key = self._institution_key(institution)
                buffers["institutions"][institution_key] = institution

            for author in paper.authors:
                author_key = self._author_key(author)
                buffers["authors"][author_key] = author
                buffers["paper_author_links"][(paper_key, author_key)] = (
                    paper_key,
                    author_key,
                    author.author_order,
                    bool(author.is_corresponding),
                )
                for institution in author.institutions:
                    institution_key = self._institution_key(institution)
                    buffers["institutions"][institution_key] = institution
                    buffers["author_institution_links"].add(
                        (author_key, institution_key)
                    )

            for topic in paper.topics:
                topic_key = self._topic_key(topic)
                buffers["topics"][topic_key] = topic
                buffers["paper_topic_links"][(paper_key, topic_key)] = (
                    paper_key,
                    topic_key,
                    self._score(topic.score),
                )

            for keyword in paper.keywords:
                keyword_key = self._keyword_key(keyword)
                buffers["keywords"][keyword_key] = keyword
                buffers["paper_keyword_links"][(paper_key, keyword_key)] = (
                    paper_key,
                    keyword_key,
                    self._score(keyword.score),
                )

            for landing in paper.landings:
                landing_key = (paper_key, landing.landing_url)
                buffers["landings"][landing_key] = landing

        return buffers

    def _upsert_keyed_buffer(
        self,
        buffer: dict[str, T],
        upsert_bulk: Callable[[list[T]], list[R]],
        batch_size: int,
    ) -> dict[str, R]:
        result: dict[str, R] = {}
        keys = list(buffer.keys())
        for key_chunk in self._chunks(keys, batch_size):
            items = [buffer[key] for key in key_chunk]
            entities = upsert_bulk(items)
            for key, entity in zip(key_chunk, entities, strict=True):
                result[key] = entity
        return result

    def _upsert_landings(
        self,
        landings: dict[tuple[str, str], ExternalLandingDTO],
        papers_by_key: dict[str, Any],
    ) -> None:
        pairs: list[tuple[int, ExternalLandingDTO]] = []
        for (paper_key, _), landing in landings.items():
            paper = papers_by_key.get(paper_key)
            if paper is not None:
                pairs.append((paper.id, landing))
        for chunk in self._chunks(pairs, self.landing_batch_size):
            self.landing_repository.upsert_bulk(chunk)

    def _attach_author_institutions(
        self,
        links: set[tuple[str, str]],
        authors_by_key: dict[str, Any],
        institutions_by_key: dict[str, Any],
    ) -> None:
        pairs: list[tuple[int, int]] = []
        for author_key, institution_key in links:
            author = authors_by_key.get(author_key)
            institution = institutions_by_key.get(institution_key)
            if author is not None and institution is not None:
                pairs.append((author.id, institution.id))
        self.institution_repository.attach_to_author_bulk(pairs)

    def _attach_paper_external_ids(
        self,
        external_ids: dict[str, str],
        papers_by_key: dict[str, Any],
    ) -> None:
        pairs: list[tuple[int, str]] = []
        for paper_key, external_id in external_ids.items():
            paper = papers_by_key.get(paper_key)
            if paper is None:
                continue
            pairs.append((paper.id, external_id))
        self.paper_meta_source_repository.attach_external_ids_bulk(
            pairs,
            self.source_name,
            self.source_prefix,
        )

    def _attach_paper_authors(
        self,
        links: dict[tuple[str, str], tuple[str, str, int | None, bool]],
        papers_by_key: dict[str, Any],
        authors_by_key: dict[str, Any],
    ) -> None:
        pairs: list[tuple[int, int, int | None, bool]] = []
        for paper_key, author_key, author_order, is_corresponding in links.values():
            paper = papers_by_key.get(paper_key)
            author = authors_by_key.get(author_key)
            if paper is not None and author is not None:
                pairs.append((paper.id, author.id, author_order, is_corresponding))
        self.author_repository.attach_to_paper_bulk(pairs)

    def _attach_paper_topics(
        self,
        links: dict[tuple[str, str], tuple[str, str, float | None]],
        papers_by_key: dict[str, Any],
        topics_by_key: dict[str, Any],
    ) -> None:
        pairs: list[tuple[int, int, float | None]] = []
        for paper_key, topic_key, score in links.values():
            paper = papers_by_key.get(paper_key)
            topic = topics_by_key.get(topic_key)
            if paper is not None and topic is not None:
                pairs.append((paper.id, topic.id, score))
        self.taxonomy_repository.attach_topics_to_papers_bulk(pairs)

    def _attach_paper_keywords(
        self,
        links: dict[tuple[str, str], tuple[str, str, float | None]],
        papers_by_key: dict[str, Any],
        keywords_by_key: dict[str, Any],
    ) -> None:
        pairs: list[tuple[int, int, float | None]] = []
        for paper_key, keyword_key, score in links.values():
            paper = papers_by_key.get(paper_key)
            keyword = keywords_by_key.get(keyword_key)
            if paper is not None and keyword is not None:
                pairs.append((paper.id, keyword.id, score))
        self.taxonomy_repository.attach_keywords_to_papers_bulk(pairs)

    def _paper_key(self, paper: ExternalPaperDTO) -> str:
        title = self._normalize_text(paper.title)
        if not title:
            raise InvalidRequestError("External paper title is required")
        return paper.doi or paper.external_id or f"{title}:{paper.publication_year or ''}"

    def _author_key(self, author: ExternalAuthorDTO) -> str:
        value = self._normalize_text(author.display_name)
        if not value:
            raise InvalidRequestError("External author display_name is required")
        return author.orcid or author.external_id or value

    def _institution_key(self, institution: ExternalInstitutionDTO) -> str:
        value = self._normalize_text(institution.display_name)
        if not value:
            raise InvalidRequestError("External institution display_name is required")
        return institution.ror or institution.external_id or value

    def _topic_key(self, topic: ExternalTopicDTO) -> str:
        name = self._normalize_text(topic.name)
        if not name:
            raise InvalidRequestError("External topic name is required")
        return topic.external_id or "|".join(
            self._normalize_text(value)
            for value in (
                topic.domain_name,
                topic.field_name,
                topic.subfield_name,
                topic.name,
            )
        )

    def _keyword_key(self, keyword: ExternalKeywordDTO) -> str:
        value = self._normalize_text(keyword.value)
        if not value:
            raise InvalidRequestError("External keyword value is required")
        return value

    def _score(self, value: Decimal | None) -> float | None:
        return float(value) if value is not None else None

    def _normalize_text(self, value: str | None) -> str:
        if value is None:
            return ""
        return " ".join(value.strip().lower().split())

    def _chunks(self, items: list[T], size: int) -> Iterable[list[T]]:
        for index in range(0, len(items), size):
            yield items[index : index + size]

    def _validate_batch_size(self, value: int, field_name: str) -> int:
        if value < 1 or value > 500:
            raise InvalidRequestError(
                "Batch size must be between 1 and 500",
                details={"field": field_name, "value": value},
            )
        return value


__all__ = ["PaperUploaderFacade"]
