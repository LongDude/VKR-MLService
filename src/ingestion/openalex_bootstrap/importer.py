from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from sqlalchemy.exc import IntegrityError

from dto.external import ExternalPaperDTO
from ingestion.openalex_bootstrap.dto import BatchImportResultDTO
from ml.facades.papers_uploading import PaperUploaderFacade
from repositories import (
    AuthorRepository,
    InstitutionRepository,
    LandingRepository,
    PaperMetaSourceRepository,
    PaperRepository,
    TaxonomyRepository,
)


class OpenAlexBatchImporter:
    """Import normalized OpenAlex papers into PostgreSQL in batches."""

    def __init__(
        self,
        *,
        session_factory: Any,
        batch_size: int = 500,
        source_name: str = "openalex",
        source_prefix: str = "https://openalex.org/",
    ) -> None:
        self.session_factory = session_factory
        self.batch_size = max(1, min(500, batch_size))
        self.source_name = source_name
        self.source_prefix = source_prefix

    async def import_papers(
        self,
        papers: list[ExternalPaperDTO],
        *,
        db_workers: int = 2,
        skip_existing: bool = False,
    ) -> BatchImportResultDTO:
        """Import papers with multiple database workers and one session per worker task."""
        prepared, result = self._prepare_papers(papers)
        if not prepared:
            return result

        chunks = list(self._chunks(prepared, self.batch_size))
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=max(1, db_workers)) as executor:
            partial_results = await asyncio.gather(
                *[
                    loop.run_in_executor(
                        executor,
                        self._import_batch_with_session,
                        chunk,
                        skip_existing,
                    )
                    for chunk in chunks
                ]
            )

        for partial in partial_results:
            self._merge_result(result, partial)
        return result

    def _prepare_papers(
        self,
        papers: list[ExternalPaperDTO],
    ) -> tuple[list[ExternalPaperDTO], BatchImportResultDTO]:
        result = BatchImportResultDTO(total=len(papers))
        deduplicated: dict[str, ExternalPaperDTO] = {}
        for paper in papers:
            if not paper.title or not paper.title.strip():
                result.skipped_empty_title += 1
                continue
            result.normalized += 1
            cleaned = self._clean_paper(paper)
            key = self._paper_key(cleaned)
            if key in deduplicated:
                result.skipped_duplicates += 1
                continue
            deduplicated[key] = cleaned
        return list(deduplicated.values()), result

    def _clean_paper(self, paper: ExternalPaperDTO) -> ExternalPaperDTO:
        authors = []
        for author in paper.authors:
            if not author.display_name or not author.display_name.strip():
                continue
            authors.append(
                author.model_copy(
                    update={
                        "institutions": [
                            institution
                            for institution in author.institutions
                            if institution.display_name
                            and institution.display_name.strip()
                        ]
                    }
                )
            )
        return paper.model_copy(
            update={
                "title": paper.title.strip(),
                "authors": authors,
                "institutions": [
                    institution
                    for institution in paper.institutions
                    if institution.display_name and institution.display_name.strip()
                ],
                "topics": [
                    topic
                    for topic in paper.topics
                    if topic.name and topic.name.strip()
                ],
                "keywords": [
                    keyword
                    for keyword in paper.keywords
                    if keyword.value and keyword.value.strip()
                ],
                "landings": [
                    landing
                    for landing in paper.landings
                    if landing.landing_url and landing.landing_url.strip()
                ],
            }
        )

    def _import_batch_with_session(
        self,
        papers: list[ExternalPaperDTO],
        skip_existing: bool,
    ) -> BatchImportResultDTO:
        with self.session_factory() as session:
            return self._import_with_fallback(session, papers, skip_existing)

    def _import_with_fallback(
        self,
        session: Any,
        papers: list[ExternalPaperDTO],
        skip_existing: bool,
    ) -> BatchImportResultDTO:
        result = BatchImportResultDTO(total=len(papers), normalized=len(papers))
        if not papers:
            return result

        paper_repository = PaperRepository(session)
        existing_keys = paper_repository.existing_external_paper_keys(
            papers,
            source_name=self.source_name,
        )
        existing_papers = [
            paper for paper in papers if self._paper_keys(paper) & existing_keys
        ]

        if skip_existing:
            papers_to_import = [
                paper for paper in papers if not (self._paper_keys(paper) & existing_keys)
            ]
            result.existing += len(existing_papers)
        else:
            papers_to_import = papers

        if not papers_to_import:
            return result

        try:
            uploader = self._uploader(session)
            upload_result = uploader.import_batch(papers_to_import)
            result.failed += upload_result.failed
            result.errors.extend(upload_result.errors)
            paper_ids = paper_repository.resolve_external_paper_ids(
                papers_to_import,
                source_name=self.source_name,
            )
            loaded_count = len(paper_ids)
            updated_count = 0
            if not skip_existing:
                updated_count = min(
                    loaded_count,
                    sum(
                        1
                        for paper in papers_to_import
                        if self._paper_keys(paper) & existing_keys
                    ),
                )
            result.updated += updated_count
            result.created += max(0, loaded_count - updated_count)
            result.paper_ids.extend(paper_ids)
            session.commit()
            return result
        except IntegrityError as exc:
            session.rollback()
            return self._fallback_after_error(
                session,
                papers_to_import,
                skip_existing,
                exc,
            )
        except Exception as exc:
            session.rollback()
            return self._fallback_after_error(
                session,
                papers_to_import,
                skip_existing,
                exc,
            )

    def _fallback_after_error(
        self,
        session: Any,
        papers: list[ExternalPaperDTO],
        skip_existing: bool,
        exc: Exception,
    ) -> BatchImportResultDTO:
        if len(papers) <= 1:
            paper = papers[0] if papers else None
            return BatchImportResultDTO(
                total=len(papers),
                normalized=len(papers),
                failed=len(papers),
                errors=[
                    {
                        "external_id": paper.external_id if paper else None,
                        "doi": paper.doi if paper else None,
                        "code": exc.__class__.__name__,
                        "message": str(exc),
                    }
                ],
            )

        midpoint = len(papers) // 2
        left = self._import_with_fallback(session, papers[:midpoint], skip_existing)
        right = self._import_with_fallback(session, papers[midpoint:], skip_existing)
        self._merge_result(left, right)
        return left

    def _uploader(self, session: Any) -> PaperUploaderFacade:
        return PaperUploaderFacade(
            paper_repository=PaperRepository(session),
            author_repository=AuthorRepository(session),
            institution_repository=InstitutionRepository(session),
            taxonomy_repository=TaxonomyRepository(session),
            landing_repository=LandingRepository(session),
            paper_meta_source_repository=PaperMetaSourceRepository(session),
            paper_batch_size=self.batch_size,
            source_name=self.source_name,
            source_prefix=self.source_prefix,
        )

    def _paper_key(self, paper: ExternalPaperDTO) -> str:
        if paper.external_id:
            return f"external:{paper.external_id}"
        if paper.doi:
            return f"doi:{paper.doi}"
        return f"title:{self._normalize_title(paper.title)}:{paper.publication_year or ''}"

    def _paper_keys(self, paper: ExternalPaperDTO) -> set[str]:
        keys: set[str] = set()
        if paper.external_id:
            keys.add(f"external:{paper.external_id}")
        if paper.doi:
            keys.add(f"doi:{paper.doi}")
        if paper.title:
            keys.add(
                f"title:{self._normalize_title(paper.title)}:{paper.publication_year or ''}"
            )
        return keys

    def _normalize_title(self, title: str) -> str:
        return " ".join(title.strip().lower().split())

    def _merge_result(
        self,
        target: BatchImportResultDTO,
        source: BatchImportResultDTO,
    ) -> None:
        target.created += source.created
        target.updated += source.updated
        target.existing += source.existing
        target.skipped_empty_title += source.skipped_empty_title
        target.skipped_duplicates += source.skipped_duplicates
        target.failed += source.failed
        target.paper_ids.extend(
            paper_id for paper_id in source.paper_ids if paper_id not in target.paper_ids
        )
        target.errors.extend(source.errors)

    def _chunks(
        self,
        items: list[ExternalPaperDTO],
        size: int,
    ) -> list[list[ExternalPaperDTO]]:
        return [items[index : index + size] for index in range(0, len(items), size)]


__all__ = ["OpenAlexBatchImporter"]
