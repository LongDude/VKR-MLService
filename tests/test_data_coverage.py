from __future__ import annotations

import sys
from datetime import date
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cli.data import (  # noqa: E402
    DataCoverageAnalyzer,
    OPENALEX_TOPIC_STATS_QUEUE,
    enqueue_missing_topic_stats_tasks,
    parse_args,
)
from cli.tasks import (  # noqa: E402
    OpenAlexTopicStatsTaskEnqueuer,
    parse_args as parse_task_args,
)
from ingestion.openalex_topic_stats import OpenAlexTopicStatsCollectionResult  # noqa: E402
from ml.workers.redis_worker import DEFAULT_QUEUE_ORDER  # noqa: E402
from ml.workers.task_handlers import MLTaskHandler  # noqa: E402


class _CapturingRedis:
    def __init__(self) -> None:
        self.messages: list[tuple[str, dict]] = []

    def enqueue(self, queue_name: str, message: dict) -> None:
        self.messages.append((queue_name, message))


class _FakeTopicStatsCollector:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def collect_and_store(self, **kwargs: object) -> OpenAlexTopicStatsCollectionResult:
        self.calls.append(dict(kwargs))
        return OpenAlexTopicStatsCollectionResult(
            date_from=kwargs["date_from"],  # type: ignore[arg-type]
            date_to=kwargs["date_to"],  # type: ignore[arg-type]
            collected=2,
        )


def test_data_coverage_months_and_missing_topic_periods() -> None:
    analyzer = DataCoverageAnalyzer(session=None, sample_limit=1)  # type: ignore[arg-type]
    months = analyzer._month_periods(date(2025, 12, 15), date(2026, 1, 1))
    topics = [
        {
            "topic_id": 1,
            "topic_name": "Graph learning",
            "field_id": 17,
            "field_name": "Computer science",
        },
        {
            "topic_id": 2,
            "topic_name": "Vision",
            "field_id": 17,
            "field_name": "Computer science",
        },
    ]

    coverage = analyzer._topic_period_coverage(
        code="publication_stats",
        description="coverage",
        period_unit="month",
        topics=topics,
        periods=months,
        existing={(1, "2025-12"), (2, "2026-01")},
    )

    assert [period["key"] for period in months] == ["2025-12", "2026-01"]
    assert months[-1]["date_to"] == date(2026, 1, 31)
    assert coverage["expected_topic_periods"] == 4
    assert coverage["covered_topic_periods"] == 2
    assert coverage["missing_topic_periods"] == 2
    assert coverage["missing_by_period"] == [
        {
            "period": "2025-12",
            "period_start": date(2025, 12, 1),
            "period_end": date(2025, 12, 31),
            "missing_topics": 1,
            "topic_sample": [
                {
                    "topic_id": 2,
                    "topic_name": "Vision",
                    "field_id": 17,
                    "field_name": "Computer science",
                }
            ],
        },
        {
            "period": "2026-01",
            "period_start": date(2026, 1, 1),
            "period_end": date(2026, 1, 31),
            "missing_topics": 1,
            "topic_sample": [
                {
                    "topic_id": 1,
                    "topic_name": "Graph learning",
                    "field_id": 17,
                    "field_name": "Computer science",
                }
            ],
        },
    ]
    assert len(coverage["missing_by_topic_sample"]) == 1


def test_data_coverage_cli_parser_accepts_field_scope() -> None:
    args = parse_args(
        [
            "analyze-coverage",
            "--field-ids",
            "17",
            "--date-from",
            "2025-01-15",
            "--date-to",
            "2025-12-01",
        ]
    )

    assert args.command == "analyze-coverage"
    assert args.field_ids == "17"
    assert args.date_to == date(2025, 12, 1)

    alias_args = parse_args(
        [
            "analyze-data-coverage",
            "--topic-ids",
            "1",
            "--date-from",
            "2025-01-01",
            "--date-to",
            "2025-01-31",
        ]
    )
    assert alias_args.command == "analyze-data-coverage"


def test_data_coverage_cli_parser_topic_stats_flags_default_to_enabled() -> None:
    args = parse_args(
        [
            "analyze-coverage",
            "--field-ids",
            "17",
            "--date-from",
            "2025-01-01",
            "--date-to",
            "2025-01-31",
        ]
    )

    assert args.normalize_january_first is True
    assert args.primary_topic_only is True

    disabled_args = parse_args(
        [
            "analyze-coverage",
            "--field-ids",
            "17",
            "--date-from",
            "2025-01-01",
            "--date-to",
            "2025-01-31",
            "--no-normalize-january-first",
            "--no-primary-topic-only",
        ]
    )
    assert disabled_args.normalize_january_first is False
    assert disabled_args.primary_topic_only is False


def test_openalex_topic_stats_queue_has_max_worker_priority() -> None:
    assert DEFAULT_QUEUE_ORDER[0] == OPENALEX_TOPIC_STATS_QUEUE


def test_topic_stats_cli_parser_topic_filter_flags_default_to_enabled() -> None:
    args = parse_task_args(
        [
            "enqueue-topic-stats",
            "--topic-ids",
            "1",
            "--date-from",
            "2025-01-01",
            "--date-to",
            "2025-01-31",
        ]
    )

    assert args.normalize_january_first is True
    assert args.primary_topic_only is True

    disabled_args = parse_task_args(
        [
            "enqueue-topic-stats",
            "--topic-ids",
            "1",
            "--date-from",
            "2025-01-01",
            "--date-to",
            "2025-01-31",
            "--no-normalize-january-first",
            "--no-primary-topic-only",
        ]
    )
    assert disabled_args.normalize_january_first is False
    assert disabled_args.primary_topic_only is False


def test_topic_stats_task_enqueuer_builds_high_priority_messages() -> None:
    redis = _CapturingRedis()
    result = OpenAlexTopicStatsTaskEnqueuer(redis_adapter=redis).enqueue(
        date_from=date(2025, 1, 1),
        date_to=date(2025, 2, 28),
        topic_ids=[1, 2, 3],
        taxonomy_scope="topic",
        task_batch_size=2,
        dry_run=False,
        show_progress=False,
    )

    assert result["queue"] == OPENALEX_TOPIC_STATS_QUEUE
    assert result["priority"] == "max"
    assert result["messages"] == 2
    assert [message["topic_ids"] for _, message in redis.messages] == [[1, 2], [3]]
    assert {message["task_type"] for _, message in redis.messages} == {
        "collect_topic_stats"
    }
    assert all(
        message["normalize_january_first"] is True for _, message in redis.messages
    )
    assert all(message["primary_topic_only"] is True for _, message in redis.messages)


def test_coverage_enqueue_missing_topic_stats_uses_full_missing_topic_ids() -> None:
    redis = _CapturingRedis()
    report = {
        "checks": {
            "publication_stats": {
                "missing_topic_periods": 3,
                "missing_by_period": [
                    {
                        "period": "2025-01",
                        "period_start": date(2025, 1, 1),
                        "period_end": date(2025, 1, 31),
                        "topic_ids": [1, 2, 3],
                    }
                ],
            }
        }
    }

    result = enqueue_missing_topic_stats_tasks(
        report,
        redis_adapter=redis,
        queue_name=OPENALEX_TOPIC_STATS_QUEUE,
        task_batch_size=2,
        languages=["en"],
        types=["article"],
        batch_size=500,
        request_workers=4,
        rate_limit_rps=10,
        max_retries=5,
    )

    assert result["messages"] == 2
    assert [message["topic_ids"] for _, message in redis.messages] == [[1, 2], [3]]
    assert all(queue == OPENALEX_TOPIC_STATS_QUEUE for queue, _ in redis.messages)
    assert all(
        message["normalize_january_first"] is True for _, message in redis.messages
    )
    assert all(message["primary_topic_only"] is True for _, message in redis.messages)


def test_worker_handler_runs_collect_topic_stats_task() -> None:
    collector = _FakeTopicStatsCollector()
    handler = MLTaskHandler(
        openalex_topic_stats_collector_factory=lambda _message: collector,
    )

    result = handler.handle(
        {
            "task_type": "collect_topic_stats",
            "date_from": "2025-01-01",
            "date_to": "2025-01-31",
            "topic_ids": [1, 2],
            "languages": ["en"],
            "types": ["article"],
            "request_workers": 2,
            "rate_limit_rps": 10,
        }
    )

    assert result.success is True
    assert result.details["task_type"] == "collect_topic_stats"
    assert collector.calls[0]["topic_ids"] == [1, 2]
    assert collector.calls[0]["date_from"] == date(2025, 1, 1)
