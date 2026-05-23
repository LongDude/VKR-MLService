from __future__ import annotations

import sys
import asyncio
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from core.exceptions import InvalidRequestError  # noqa: E402
from dto.external import ExternalPaperDTO, ExternalTopicDTO, OpenAlexSearchFiltersDTO  # noqa: E402
from dto.openalex import (  # noqa: E402
    BatchImportResultDTO,
    OpenAlexPlanUnitDTO,
    OpenAlexUnitSummaryDTO,
)
from ingestion.openalex_bootstrap import (  # noqa: E402
    OpenAlexBatchImporter,
    OpenAlexBootstrapDownloader,
    OpenAlexBootstrapRequestDTO,
    OpenAlexBootstrapTopicTargetDTO,
    OpenAlexLoadPlanDTO,
    OpenAlexLoadPlanBuilder,
    OpenAlexLoadPlanItemDTO,
    OpenAlexPendingPageDTO,
)
from cli.openalex import parse_args, resolve_required_openalex_api_key, run_bootstrap_papers  # noqa: E402
from ingestion.openalex_topic_stats import OpenAlexTopicStatsCollector  # noqa: E402
from ml.facades.openalex_papers import OpenAlexPapersFacade  # noqa: E402
from ml.facades.papers_uploading import PaperUploaderFacade  # noqa: E402
from ml.services.openalex_paper_downloader import OpenAlexDownloadResult  # noqa: E402
from ml.services.openalex_paper_plan import (  # noqa: E402
    OPENALEX_MAX_SAMPLE,
    OpenAlexPaperPlanService,
)
from ml.services.openalex_periods import OpenAlexPeriodService  # noqa: E402


def test_monthly_load_plan_combines_languages_into_one_openalex_filter(
    tmp_path: Path,
) -> None:
    request = OpenAlexBootstrapRequestDTO(
        target_count=300,
        target_count_scope="period",
        date_from=date(2026, 1, 1),
        date_to=date(2026, 3, 31),
        normalize="none",
        languages=["en", "ru"],
        types=["article"],
        openalex_filter_parts=["topics.field.id:41008148"],
        per_page=100,
    )

    plan = OpenAlexLoadPlanBuilder().build(request, target_new_count=300, seed=42)

    assert plan.estimated_requests == 3
    assert len(plan.items) == 3
    assert [item.sample_size for item in plan.items] == [100, 100, 100]
    assert plan.items[0].date_from == date(2026, 1, 2)
    assert plan.items[-1].date_to == date(2026, 3, 31)
    assert {item.language for item in plan.items} == {"en|ru"}
    assert {item.type for item in plan.items} == {"article"}
    filter_value = OpenAlexBootstrapDownloader()._filter_param(plan.items[0])
    assert "language:en|ru" in filter_value
    assert "has_abstract:true" in filter_value
    assert "topics.field.id:41008148" in filter_value


def test_openalex_bootstrap_requires_api_key_for_real_runs(monkeypatch: Any) -> None:
    monkeypatch.delenv("OPENALEX_API_KEY", raising=False)

    with pytest.raises(ValueError):
        resolve_required_openalex_api_key(None, dry_run=False)

    assert resolve_required_openalex_api_key(None, dry_run=True) is None


def test_openalex_bootstrap_downloader_sends_auth_params() -> None:
    captured_params: list[httpx.QueryParams] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_params.append(request.url.params)
        return httpx.Response(200, json={"results": []})

    downloader = OpenAlexBootstrapDownloader(
        request_workers=1,
        api_key="api-token",
        mailto="owner@example.com",
        transport=httpx.MockTransport(handler),
    )
    plan = OpenAlexLoadPlanDTO(
        items=[
            OpenAlexLoadPlanItemDTO(
                date_from=date(2026, 1, 1),
                date_to=date(2026, 1, 31),
                sample_size=1,
                language="en",
                type="article",
                seed=42,
            )
        ],
        estimated_requests=1,
    )

    asyncio.run(downloader.fetch_plan(plan, sample=False, show_progress=False))

    assert captured_params
    assert captured_params[0]["api_key"] == "api-token"
    assert captured_params[0]["mailto"] == "owner@example.com"


def test_openalex_period_service_rounds_to_months_and_handles_january() -> None:
    service = OpenAlexPeriodService()

    months = service.scope_periods(
        date_from=date(2025, 12, 15),
        date_to=date(2026, 1, 1),
        target_scope="month",
    )
    years = service.scope_periods(
        date_from=date(2026, 1, 15),
        date_to=date(2026, 12, 1),
        target_scope="year",
    )

    assert [(item.key, item.date_from, item.date_to) for item in months] == [
        ("2025-12", date(2025, 12, 1), date(2025, 12, 31)),
        ("2026-01", date(2026, 1, 2), date(2026, 1, 31)),
    ]
    assert [(item.key, item.date_from, item.date_to) for item in years] == [
        ("2026", date(2026, 1, 1), date(2026, 12, 31)),
    ]


def test_openalex_plan_rejects_quota_unit_above_max_sample() -> None:
    request = OpenAlexBootstrapRequestDTO(
        target_count=OPENALEX_MAX_SAMPLE + 1,
        target_count_scope="month",
        date_from=date(2026, 1, 1),
        date_to=date(2026, 1, 31),
        languages=["en"],
        types=["article"],
    )

    with pytest.raises(InvalidRequestError):
        OpenAlexPaperPlanService().build(request, seed=42)


def test_openalex_plan_allows_large_total_when_each_quota_unit_is_valid() -> None:
    request = OpenAlexBootstrapRequestDTO(
        target_count=6_000,
        target_count_scope="month",
        target_count_unit="topic",
        topic_targets=[
            OpenAlexBootstrapTopicTargetDTO(topic_id=1, filter_part="topics.id:T1"),
            OpenAlexBootstrapTopicTargetDTO(topic_id=2, filter_part="topics.id:T2"),
        ],
        date_from=date(2026, 1, 1),
        date_to=date(2026, 2, 28),
        languages=["en"],
        types=["article"],
        openalex_filter_parts=["topics.field.id:F1"],
        per_page=100,
    )

    plan = OpenAlexPaperPlanService().build(request, seed=42)

    assert plan.total_sample_count == 24_000
    assert len(plan.units) == 4
    assert all(unit.requested == 6_000 for unit in plan.units)
    assert [item.date_from for item in plan.items[:2]] == [
        date(2026, 1, 2),
        date(2026, 1, 2),
    ]
    assert {tuple(item.filter_parts) for item in plan.items} == {
        ("topics.field.id:F1", "topics.id:T1"),
        ("topics.field.id:F1", "topics.id:T2"),
    }


def test_openalex_bootstrap_downloader_defers_long_rate_limit() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        page = request.url.params.get("page")
        if page == "1":
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "id": "https://openalex.org/W1",
                            "title": "Paper one",
                            "abstract_inverted_index": {"Abstract": [0]},
                        }
                    ]
                },
            )
        return httpx.Response(429, headers={"Retry-After": "121"}, text="limited")

    downloader = OpenAlexBootstrapDownloader(
        request_workers=1,
        max_retries=5,
        rate_limit_defer_after_seconds=120,
        transport=httpx.MockTransport(handler),
    )
    pages = [
        OpenAlexPendingPageDTO(
            date_from=date(2026, 1, 1),
            date_to=date(2026, 1, 31),
            sample_size=200,
            language="en",
            type="article",
            seed=42,
            page=1,
            per_page=100,
        ),
        OpenAlexPendingPageDTO(
            date_from=date(2026, 1, 1),
            date_to=date(2026, 1, 31),
            sample_size=200,
            language="en",
            type="article",
            seed=42,
            page=2,
            per_page=100,
        ),
    ]

    result = asyncio.run(downloader.fetch_pages(pages, show_progress=False))

    assert calls == 2
    assert result.deferred is True
    assert result.retry_after_seconds == 121
    assert result.normalized == 1
    assert [page.page for page in result.pending_pages] == [2]


def test_openalex_downloader_marks_short_page_as_exhausted_without_count_request() -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "id": "https://openalex.org/W1",
                        "title": "Paper one",
                        "abstract_inverted_index": {"Abstract": [0]},
                    }
                ]
            },
        )

    downloader = OpenAlexBootstrapDownloader(
        request_workers=1,
        transport=httpx.MockTransport(handler),
    )
    result = asyncio.run(
        downloader.fetch_pages(
            [
                OpenAlexPendingPageDTO(
                    date_from=date(2026, 2, 1),
                    date_to=date(2026, 2, 28),
                    sample_size=2,
                    language="en",
                    type="article",
                    seed=42,
                    page=1,
                    per_page=2,
                    unit_key="2026-02",
                    period="2026-02",
                    requested_count=2,
                )
            ],
            show_progress=False,
        )
    )

    assert paths == ["/works"]
    assert result.unit_summaries["2026-02"].exhausted is True
    assert result.unit_summaries["2026-02"].reason == "short_page"


def test_openalex_downloader_full_final_partial_page_is_not_exhausted() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "id": "https://openalex.org/W3",
                        "title": "Paper three",
                        "abstract_inverted_index": {"Abstract": [0]},
                    }
                ]
            },
        )

    downloader = OpenAlexBootstrapDownloader(
        request_workers=1,
        transport=httpx.MockTransport(handler),
    )
    result = asyncio.run(
        downloader.fetch_pages(
            [
                OpenAlexPendingPageDTO(
                    date_from=date(2026, 2, 1),
                    date_to=date(2026, 2, 28),
                    sample_size=3,
                    language="en",
                    type="article",
                    seed=42,
                    page=2,
                    per_page=2,
                    unit_key="2026-02",
                    period="2026-02",
                    requested_count=3,
                )
            ],
            show_progress=False,
        )
    )

    assert result.unit_summaries["2026-02"].exhausted is False


def test_openalex_importer_skips_missing_title_or_abstract() -> None:
    importer = OpenAlexBatchImporter(session_factory=lambda: None)

    prepared, result = importer._prepare_papers(
        [
            ExternalPaperDTO(
                external_id="https://openalex.org/W1",
                title=" Valid ",
                abstract=" Abstract ",
            ),
            ExternalPaperDTO(
                external_id="https://openalex.org/W2",
                title="No abstract",
                abstract=None,
            ),
            ExternalPaperDTO(
                external_id="https://openalex.org/W3",
                title="",
                abstract="Has abstract",
            ),
        ]
    )

    assert [paper.title for paper in prepared] == ["Valid"]
    assert prepared[0].abstract == "Abstract"
    assert result.normalized == 1
    assert result.skipped_empty_title == 1
    assert result.skipped_empty_abstract == 1


class _FakeBootstrapSession:
    def __enter__(self) -> "_FakeBootstrapSession":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None


class _FakeFacadePaperRepository:
    def __init__(self, session: _FakeBootstrapSession) -> None:
        self.session = session

    def count_all(self) -> int:
        return 0

    def existing_external_paper_keys(
        self,
        papers: list[ExternalPaperDTO],
        *,
        source_name: str,
    ) -> set[str]:
        return {"external:https://openalex.org/W1"}


class _OnePassPlanService:
    def build(
        self,
        request: OpenAlexBootstrapRequestDTO,
        *,
        seed: int,
    ) -> OpenAlexLoadPlanDTO:
        return OpenAlexLoadPlanDTO(
            items=[
                OpenAlexLoadPlanItemDTO(
                    date_from=request.date_from,
                    date_to=request.date_to,
                    sample_size=request.target_count,
                    language=request.languages[0],
                    type=request.types[0],
                    filter_parts=[],
                    seed=seed,
                    unit_key="u1",
                    period="2026-02",
                    requested_count=request.target_count,
                )
            ],
            units=[
                OpenAlexPlanUnitDTO(
                    unit_key="u1",
                    period="2026-02",
                    date_from=request.date_from,
                    date_to=request.date_to,
                    requested=request.target_count,
                )
            ],
            total_sample_count=request.target_count,
            estimated_requests=1,
        )


class _OnePassDownloader:
    def __init__(self) -> None:
        self.calls = 0

    async def fetch_plan(
        self,
        plan: OpenAlexLoadPlanDTO,
        *,
        sample: bool,
        per_page: int,
        show_progress: bool,
    ) -> OpenAlexDownloadResult:
        self.calls += 1
        return OpenAlexDownloadResult(
            papers=[
                ExternalPaperDTO(
                    external_id="https://openalex.org/W1",
                    title="Paper one",
                    abstract="Abstract",
                )
            ],
            fetched=1,
            normalized=1,
            unit_summaries={
                "u1": OpenAlexUnitSummaryDTO(
                    unit_key="u1",
                    period="2026-02",
                    requested=1,
                    fetched=1,
                )
            },
            paper_unit_keys={"external:https://openalex.org/W1": ["u1"]},
            openalex_requests=1,
        )


class _DuplicateImporter:
    async def import_papers(
        self,
        papers: list[ExternalPaperDTO],
        *,
        db_workers: int,
        skip_existing: bool,
        show_progress: bool,
    ) -> BatchImportResultDTO:
        return BatchImportResultDTO(
            total=len(papers),
            normalized=len(papers),
            updated=len(papers),
            paper_ids=[1],
        )


class _CapturingImporter:
    def __init__(self) -> None:
        self.papers: list[ExternalPaperDTO] = []

    async def import_papers(
        self,
        papers: list[ExternalPaperDTO],
        *,
        db_workers: int,
        skip_existing: bool,
        show_progress: bool,
    ) -> BatchImportResultDTO:
        self.papers = papers
        return BatchImportResultDTO(
            total=len(papers),
            normalized=len(papers),
            created=len(papers),
            paper_ids=[1],
        )


def test_openalex_facade_runs_one_pass_and_reports_duplicate_shortfall(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        "ml.facades.openalex_papers.PaperRepository",
        _FakeFacadePaperRepository,
    )
    downloader = _OnePassDownloader()
    facade = OpenAlexPapersFacade(
        session_factory=lambda: _FakeBootstrapSession(),
        plan_service=_OnePassPlanService(),  # type: ignore[arg-type]
        downloader=downloader,  # type: ignore[arg-type]
        importer=_DuplicateImporter(),  # type: ignore[arg-type]
    )

    report = asyncio.run(
        facade.bootstrap(
            OpenAlexBootstrapRequestDTO(
                target_count=1,
                target_count_scope="month",
                date_from=date(2026, 2, 1),
                date_to=date(2026, 2, 28),
                languages=["en"],
                types=["article"],
            )
        )
    )

    assert downloader.calls == 1
    assert report.duplicate_shortfall == 1
    assert len(report.rounds) == 1


def test_openalex_facade_sets_primary_topic_from_topic_filter(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(
        "ml.facades.openalex_papers.PaperRepository",
        _FakeFacadePaperRepository,
    )
    importer = _CapturingImporter()
    downloader = _OnePassDownloader()
    facade = OpenAlexPapersFacade(
        session_factory=lambda: _FakeBootstrapSession(),
        plan_service=_OnePassPlanService(),  # type: ignore[arg-type]
        downloader=downloader,  # type: ignore[arg-type]
        importer=importer,  # type: ignore[arg-type]
    )

    downloader.fetch_plan = _fetch_one_topic_plan  # type: ignore[method-assign]
    asyncio.run(
        facade.bootstrap(
            OpenAlexBootstrapRequestDTO(
                target_count=1,
                target_count_scope="month",
                target_count_unit="topic",
                topic_targets=[
                    OpenAlexBootstrapTopicTargetDTO(
                        topic_id=42,
                        filter_part="topics.id:T42",
                    )
                ],
                date_from=date(2026, 2, 1),
                date_to=date(2026, 2, 28),
                languages=["en"],
                types=["article"],
            )
        )
    )

    assert importer.papers[0].primary_topic_id == 42


async def _fetch_one_topic_plan(
    plan: OpenAlexLoadPlanDTO,
    *,
    sample: bool,
    per_page: int,
    show_progress: bool,
) -> OpenAlexDownloadResult:
    return OpenAlexDownloadResult(
        papers=[
            ExternalPaperDTO(
                external_id="https://openalex.org/W42",
                title="Paper topic",
                abstract="Abstract",
            )
        ],
        fetched=1,
        normalized=1,
        unit_summaries={
            "u42": OpenAlexUnitSummaryDTO(
                unit_key="u42",
                topic_id=42,
                period="2026-02",
                requested=1,
                fetched=1,
            )
        },
        paper_unit_keys={"external:https://openalex.org/W42": ["u42"]},
        openalex_requests=1,
    )


def test_paper_uploader_falls_back_to_highest_score_primary_topic() -> None:
    uploader = PaperUploaderFacade(
        paper_repository=SimpleNamespace(),
        author_repository=SimpleNamespace(),
        institution_repository=SimpleNamespace(),
        taxonomy_repository=SimpleNamespace(),
        landing_repository=SimpleNamespace(),
    )
    paper = SimpleNamespace(id=1, primary_topic_id=None)

    uploader._assign_primary_topics(
        {
            "paper": ExternalPaperDTO(
                title="Paper",
                topics=[
                    ExternalTopicDTO(name="Low", score=0.1),
                    ExternalTopicDTO(name="High", score=0.9),
                ],
            )
        },
        {
            ("paper", "low"): ("paper", "low", 0.1),
            ("paper", "high"): ("paper", "high", 0.9),
        },
        {"paper": paper},
        {
            "low": SimpleNamespace(id=10),
            "high": SimpleNamespace(id=20),
        },
    )

    assert paper.primary_topic_id == 20


def test_openalex_cli_accepts_normalize_none_and_rejects_extra_rounds() -> None:
    args = parse_args(
        [
            "bootstrap-papers",
            "--target-count",
            "1",
            "--date-from",
            "2026-02-01",
            "--date-to",
            "2026-02-28",
            "--normalize",
            "none",
            "--max-rounds",
            "2",
            "--dry-run",
        ]
    )

    with pytest.raises(ValueError, match="--max-rounds"):
        asyncio.run(run_bootstrap_papers(args))


class _NoopRateLimiter:
    def acquire(self) -> None:
        return None


class _FakeTopicRepository:
    def list_topics_by_ids(self, topic_ids: list[int]) -> list[Any]:
        return [
            SimpleNamespace(id=topic_id, openalex_id=f"https://openalex.org/T{topic_id}", name="AI")
            for topic_id in topic_ids
        ]


class _FakeTopicStatsRepository:
    def __init__(self) -> None:
        self.session = SimpleNamespace(commit=lambda: None)
        self.items: list[Any] = []

    def upsert_many(self, items: list[Any]) -> tuple[int, int]:
        self.items.extend(items)
        return len(items), 0


class _FakeYearlyStatsRepository:
    def __init__(self) -> None:
        self.items: list[Any] = []

    def upsert_artificial_estimates(self, items: list[Any]) -> tuple[int, int]:
        self.items.extend(items)
        return len(items), 0


class _FakeOpenAlexAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[OpenAlexSearchFiltersDTO, str | None]] = []

    def count_works(
        self,
        filters: OpenAlexSearchFiltersDTO,
        *,
        topic_external_id: str | None = None,
        primary_topic_only: bool = False,
    ) -> int:
        self.calls.append((filters, topic_external_id))
        return 7


def test_collect_topic_stats_combines_languages_into_one_count_request() -> None:
    stats_repository = _FakeTopicStatsRepository()
    adapter = _FakeOpenAlexAdapter()
    collector = OpenAlexTopicStatsCollector(
        taxonomy_repository=_FakeTopicRepository(),  # type: ignore[arg-type]
        stats_repository=stats_repository,  # type: ignore[arg-type]
        openalex_adapter_factory=lambda: adapter,  # type: ignore[arg-type]
        request_workers=1,
        rate_limiter=_NoopRateLimiter(),  # type: ignore[arg-type]
    )

    result = collector.collect_and_store(
        date_from=date(2026, 1, 1),
        date_to=date(2026, 2, 28),
        topic_ids=[1],
        languages=["en", "ru"],
        types=["article"],
        show_progress=False,
    )

    assert result.planned_requests == 2
    assert result.openalex_requests == 2
    assert len(adapter.calls) == 2
    assert {call[0].language for call in adapter.calls} == {"en|ru"}
    assert {call[0].type for call in adapter.calls} == {"article"}
    assert [item.works_count for item in stats_repository.items] == [7, 7]


class _FakeGroupedTopicRepository:
    def list_topics_for_stats(
        self,
        limit: int | None = None,
        offset: int = 0,
        field_ids: list[int] | None = None,
        subfield_ids: list[int] | None = None,
    ) -> list[Any]:
        return [
            SimpleNamespace(id=1, openalex_id="https://openalex.org/T1", name="AI"),
            SimpleNamespace(id=2, openalex_id="https://openalex.org/T2", name="NLP"),
        ]


class _FakeGroupedOpenAlexAdapter:
    def __init__(self) -> None:
        self.calls: list[tuple[OpenAlexSearchFiltersDTO, str]] = []

    def group_works(
        self,
        filters: OpenAlexSearchFiltersDTO,
        *,
        group_by: str,
        extra_filter_parts: list[str] | None = None,
        cursor: str = "*",
        per_page: int = 200,
    ) -> tuple[list[dict[str, Any]], str | None]:
        self.calls.append((filters, group_by))
        if filters.date_from == filters.date_to:
            return ([{"key": "https://openalex.org/T1", "count": 100}], None)
        return (
            [
                {"key": "https://openalex.org/T1", "count": 310},
                {"key": "https://openalex.org/T2", "count": 10},
            ],
            None,
        )


def test_collect_topic_stats_field_scope_uses_group_by_and_normalizes_january() -> None:
    stats_repository = _FakeTopicStatsRepository()
    yearly_repository = _FakeYearlyStatsRepository()
    adapter = _FakeGroupedOpenAlexAdapter()
    collector = OpenAlexTopicStatsCollector(
        taxonomy_repository=_FakeGroupedTopicRepository(),  # type: ignore[arg-type]
        stats_repository=stats_repository,  # type: ignore[arg-type]
        yearly_stats_repository=yearly_repository,  # type: ignore[arg-type]
        openalex_adapter_factory=lambda: adapter,  # type: ignore[arg-type]
        request_workers=1,
        rate_limiter=_NoopRateLimiter(),  # type: ignore[arg-type]
    )

    result = collector.collect_and_store(
        date_from=date(2026, 1, 1),
        date_to=date(2026, 1, 31),
        taxonomy_scope="field",
        languages=["en", "ru"],
        types=["article"],
        normalize_january_first=True,
        show_progress=False,
    )

    assert result.planned_requests == 2
    assert result.openalex_requests == 2
    assert [call[1] for call in adapter.calls] == ["topics.id", "topics.id"]
    assert [(item.topic_id, item.works_count) for item in stats_repository.items] == [
        (1, 217),
        (2, 10),
    ]
    assert [
        (item.topic_id, item.artifical_pubdates_estimation)
        for item in yearly_repository.items
    ] == [(1, 93), (2, 0)]
