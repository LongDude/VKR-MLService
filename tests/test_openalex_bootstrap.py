from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dto.external import OpenAlexSearchFiltersDTO  # noqa: E402
from ingestion.openalex_bootstrap import (  # noqa: E402
    OpenAlexBootstrapDownloader,
    OpenAlexBootstrapRequestDTO,
    OpenAlexLoadPlanBuilder,
)
from ingestion.openalex_topic_stats import OpenAlexTopicStatsCollector  # noqa: E402


def test_monthly_load_plan_combines_languages_into_one_openalex_filter(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "monthly-counts.csv"
    csv_path.write_text(
        "\n".join(
            [
                "period,count",
                "2026-01,100",
                "2026-02,100",
                "2026-03,100",
            ]
        ),
        encoding="utf-8",
    )
    request = OpenAlexBootstrapRequestDTO(
        target_count=300,
        date_from=date(2026, 1, 1),
        date_to=date(2026, 3, 31),
        normalize="month",
        monthly_stats_source="csv",
        monthly_counts_csv=str(csv_path),
        languages=["en", "ru"],
        types=["article"],
        per_page=100,
    )

    plan = OpenAlexLoadPlanBuilder().build(request, target_new_count=300, seed=42)

    assert plan.estimated_requests == 3
    assert len(plan.items) == 3
    assert [item.sample_size for item in plan.items] == [100, 100, 100]
    assert {item.language for item in plan.items} == {"en|ru"}
    assert {item.type for item in plan.items} == {"article"}
    assert "language:en|ru" in OpenAlexBootstrapDownloader()._filter_param(plan.items[0])


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
