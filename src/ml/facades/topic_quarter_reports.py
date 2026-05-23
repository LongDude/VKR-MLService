from __future__ import annotations

from collections import Counter
from datetime import date
from decimal import Decimal
import json
from typing import Any

from pydantic import ValidationError

from adapters.lmstudio_chat_adapter import LMStudioChatAdapter
from core.exceptions import AppError, EntityNotFoundError, LLMGenerationError
from dto.common import BatchOperationResultDTO, OperationResultDTO
from dto.topic_reports import (
    TopicQuarterReportGenerateRequestDTO,
    TopicQuarterReportItemInputDTO,
    TopicQuarterReportPaperInputDTO,
)
from ml.constants import DEFAULT_CHAT_MODEL
from ml.services.events import EventSink, MLEvent, NoopEventSink
from ml.services.quarter_periods import QuarterPeriodService
from ml.services.scoring import ScoringService
from ml.services.topic_quarter_report_prompts import TopicQuarterReportPromptService
from repositories.openalex_topic_stats import OpenAlexTopicStatsRepository
from repositories.papers import PaperRepository
from repositories.research_clusters import ResearchClusterRepository
from repositories.taxonomy import TaxonomyRepository
from repositories.topic_quarter_reports import TopicQuarterReportRepository


REPRESENTATIVE_PAPER_LIMIT = 8
TOP_CITED_PAPER_LIMIT = 5
ABSTRACT_CHAR_LIMIT = 1400


class TopicQuarterReportFacade:
    """Generate and persist quarterly LLM reports for OpenAlex topics."""

    def __init__(
        self,
        *,
        taxonomy_repository: TaxonomyRepository,
        paper_repository: PaperRepository,
        research_cluster_repository: ResearchClusterRepository,
        topic_report_repository: TopicQuarterReportRepository,
        openalex_topic_stats_repository: OpenAlexTopicStatsRepository,
        chat_adapter: LMStudioChatAdapter,
        prompt_service: TopicQuarterReportPromptService | None = None,
        quarter_period_service: QuarterPeriodService | None = None,
        scoring_service: ScoringService | None = None,
        event_sink: EventSink | None = None,
        chat_model: str = DEFAULT_CHAT_MODEL,
    ) -> None:
        self.taxonomy_repository = taxonomy_repository
        self.paper_repository = paper_repository
        self.research_cluster_repository = research_cluster_repository
        self.topic_report_repository = topic_report_repository
        self.openalex_topic_stats_repository = openalex_topic_stats_repository
        self.chat_adapter = chat_adapter
        self.prompt_service = prompt_service or TopicQuarterReportPromptService()
        self.quarter_period_service = quarter_period_service or QuarterPeriodService()
        self.scoring_service = scoring_service or ScoringService()
        self.event_sink = event_sink or NoopEventSink()
        self.chat_model = chat_model

    def generate_one(
        self,
        request: TopicQuarterReportGenerateRequestDTO,
    ) -> OperationResultDTO:
        """Generate or skip one quarterly topic report."""
        period_key = self.quarter_period_service.period_key(request.period_start)
        self._emit(
            "topic_quarter_report_started",
            entity_id=f"topic:{request.topic_id}:{period_key}",
            stage="report",
            current=0,
            total=1,
            message="Topic quarter report generation started",
        )
        topic = self.taxonomy_repository.get_topic_by_id(request.topic_id)
        if topic is None:
            raise EntityNotFoundError(
                "Topic not found",
                details={"topic_id": request.topic_id},
            )

        existing = self.topic_report_repository.get_by_topic_period(
            request.topic_id,
            period_key,
        )
        if existing is not None and not request.force:
            return self._skipped_result(
                request,
                period_key,
                reason="report_exists",
                report_id=int(existing.id),
            )

        period_stats = self.research_cluster_repository.list_period_stats(
            f"topic:{request.topic_id}",
            request.period_start,
            request.period_end,
        )
        if not period_stats:
            return self._skipped_result(
                request,
                period_key,
                reason="missing_cluster_period_stats",
            )

        metrics = self._metrics(period_stats)
        keyword_dynamics = self._keyword_dynamics(period_stats)
        openalex_counts = self.openalex_topic_stats_repository.load_period_stats(
            date_from=request.period_start,
            date_to=request.period_end,
            granularity="month",
            topic_ids=[request.topic_id],
        )
        representative_papers = self._representative_papers(
            request.topic_id,
            request.period_start,
            request.period_end,
            period_stats,
        )
        response_payload = self._generate_payload(
            topic={
                "id": int(topic.id),
                "name": topic.name,
                "openalex_id": topic.openalex_id,
            },
            period={
                "period_key": period_key,
                "period_start": request.period_start.isoformat(),
                "period_end": request.period_end.isoformat(),
            },
            metrics=metrics,
            keyword_dynamics=keyword_dynamics,
            openalex_counts=openalex_counts,
            representative_papers=representative_papers,
            report_language=request.report_language,
        )
        report, created = self.topic_report_repository.upsert_report(
            topic_id=request.topic_id,
            period_start=request.period_start,
            period_end=request.period_end,
            period_key=period_key,
            title=self._clean_text(response_payload.get("title")),
            summary=self._clean_text(response_payload.get("summary")),
            definition=self._clean_text(response_payload.get("definition")),
            dynamics_summary=self._clean_text(response_payload.get("dynamics_summary")),
            future_dynamics=self._clean_text(response_payload.get("future_dynamics")),
            metrics=metrics,
            keyword_dynamics=keyword_dynamics,
        )
        items = self._items_from_payload(response_payload)
        self.topic_report_repository.replace_items(int(report.id), items)
        paper_links = self._paper_links(representative_papers)
        self.topic_report_repository.replace_papers(int(report.id), paper_links)
        self._emit(
            "topic_quarter_report_completed",
            entity_id=f"topic:{request.topic_id}:{period_key}",
            stage="report",
            current=1,
            total=1,
            message="Topic quarter report generation completed",
            payload={"report_id": int(report.id), "created": created},
        )
        return OperationResultDTO(
            success=True,
            message="Topic quarter report generated",
            details={
                "task_type": "topic_quarter_report",
                "status": "created" if created else "updated",
                "topic_id": request.topic_id,
                "period_key": period_key,
                "report_id": int(report.id),
                "items": len(items),
                "papers": len(paper_links),
            },
        )

    def generate_many(
        self,
        requests: list[TopicQuarterReportGenerateRequestDTO],
    ) -> BatchOperationResultDTO:
        """Generate reports for many topic-quarter requests."""
        result = BatchOperationResultDTO(total=len(requests))
        self._emit(
            "topic_quarter_report_batch_started",
            entity_id="batch",
            stage="reports",
            current=0,
            total=len(requests),
            message=f"Generating {len(requests)} topic quarter reports",
        )
        for index, request in enumerate(requests, start=1):
            try:
                item = self.generate_one(request)
            except AppError as exc:
                result.failed += 1
                result.errors.append(
                    {
                        "topic_id": request.topic_id,
                        "period_start": request.period_start.isoformat(),
                        "period_end": request.period_end.isoformat(),
                        "code": exc.code,
                        "message": exc.message,
                        "details": exc.details or {},
                    }
                )
            except Exception as exc:
                result.failed += 1
                result.errors.append(
                    {
                        "topic_id": request.topic_id,
                        "period_start": request.period_start.isoformat(),
                        "period_end": request.period_end.isoformat(),
                        "code": exc.__class__.__name__,
                        "message": str(exc),
                        "details": {},
                    }
                )
            else:
                status = item.details.get("status")
                if status == "created":
                    result.created += 1
                elif status == "updated":
                    result.updated += 1
                else:
                    result.skipped += 1
            self._emit(
                "topic_quarter_report_batch_progress",
                entity_id="batch",
                stage="reports",
                current=index,
                total=len(requests),
                message=(
                    f"done={index} created={result.created} updated={result.updated} "
                    f"skipped={result.skipped} failed={result.failed}"
                ),
            )

        self._emit(
            "topic_quarter_report_batch_completed",
            entity_id="batch",
            stage="reports",
            current=len(requests),
            total=len(requests),
            message="Topic quarter report batch completed",
            payload=result.model_dump(mode="json"),
        )
        return result

    def _skipped_result(
        self,
        request: TopicQuarterReportGenerateRequestDTO,
        period_key: str,
        *,
        reason: str,
        report_id: int | None = None,
    ) -> OperationResultDTO:
        self._emit(
            "topic_quarter_report_skipped",
            entity_id=f"topic:{request.topic_id}:{period_key}",
            stage="report",
            current=1,
            total=1,
            message=f"Topic quarter report skipped: {reason}",
            payload={"reason": reason, "report_id": report_id},
        )
        return OperationResultDTO(
            success=True,
            message="Topic quarter report skipped",
            details={
                "task_type": "topic_quarter_report",
                "status": "skipped",
                "reason": reason,
                "topic_id": request.topic_id,
                "period_key": period_key,
                "report_id": report_id,
            },
        )

    def _metrics(self, period_stats: list[Any]) -> dict[str, Any]:
        paper_count = sum(int(stat.paper_count or 0) for stat in period_stats)
        previous_paper_count = sum(
            int(stat.previous_paper_count or 0) for stat in period_stats
        )
        citation_count_sum = sum(
            int(stat.citation_count_sum or 0) for stat in period_stats
        )
        return {
            "paper_count": paper_count,
            "previous_paper_count": previous_paper_count,
            "growth_rate": self.scoring_service.calculate_growth_rate(
                paper_count,
                previous_paper_count,
            ),
            "trend_score": self._weighted_average(
                period_stats,
                "trend_score",
                weight_field="paper_count",
            ),
            "semantic_drift": self._average(period_stats, "semantic_drift"),
            "citation_count_sum": citation_count_sum,
            "avg_cited_by_count": (
                citation_count_sum / paper_count if paper_count > 0 else None
            ),
        }

    def _keyword_dynamics(self, period_stats: list[Any]) -> dict[str, Any]:
        total_counter: Counter[str] = Counter()
        first_counter: Counter[str] = Counter()
        last_counter: Counter[str] = Counter()
        periods_by_keyword: dict[str, set[str]] = {}
        for index, stat in enumerate(period_stats):
            keywords = self._string_list(stat.top_keywords)
            period_key = stat.period_start.isoformat()
            counter = Counter(keywords)
            total_counter.update(counter)
            if index == 0:
                first_counter.update(counter)
            if index == len(period_stats) - 1:
                last_counter.update(counter)
            for keyword in keywords:
                periods_by_keyword.setdefault(keyword, set()).add(period_key)

        stable = [
            keyword
            for keyword, periods in periods_by_keyword.items()
            if len(periods) >= 2
        ]
        emerging = [
            keyword
            for keyword in last_counter
            if keyword not in first_counter
        ]
        declining = [
            keyword
            for keyword in first_counter
            if keyword not in last_counter
        ]
        return {
            "top_keywords": [
                keyword for keyword, _ in total_counter.most_common(20)
            ],
            "stable_keywords": stable[:20],
            "emerging_keywords": emerging[:20],
            "declining_keywords": declining[:20],
            "period_keyword_counts": {
                stat.period_start.isoformat(): self._string_list(stat.top_keywords)
                for stat in period_stats
            },
        }

    def _representative_papers(
        self,
        topic_id: int,
        period_start: date,
        period_end: date,
        period_stats: list[Any],
    ) -> list[dict[str, Any]]:
        representative_ids: list[int] = []
        for stat in period_stats:
            for paper_id in self._int_list(stat.representative_paper_ids):
                if paper_id not in representative_ids:
                    representative_ids.append(paper_id)
        representative_ids = representative_ids[:REPRESENTATIVE_PAPER_LIMIT]
        papers_by_id = {
            int(paper.id): paper
            for paper in self.paper_repository.get_by_ids(representative_ids)
        }
        records: list[dict[str, Any]] = []
        for index, paper_id in enumerate(representative_ids, start=1):
            paper = papers_by_id.get(paper_id)
            if paper is None:
                continue
            records.append(
                {
                    **self._paper_payload(paper),
                    "roles": ["representative"],
                    "score": 1 / index,
                }
            )

        seen_ids = {int(record["id"]) for record in records}
        for index, paper in enumerate(
            self.paper_repository.list_top_cited_by_topic_and_period(
                topic_id,
                period_start,
                period_end,
                limit=TOP_CITED_PAPER_LIMIT,
            ),
            start=1,
        ):
            paper_id = int(paper.id)
            role_score = 1 / index
            if paper_id in seen_ids:
                for record in records:
                    if int(record["id"]) == paper_id:
                        record["roles"].append("highly_cited")
                        record["score"] = max(float(record["score"]), role_score)
                continue
            records.append(
                {
                    **self._paper_payload(paper),
                    "roles": ["highly_cited"],
                    "score": role_score,
                }
            )
            seen_ids.add(paper_id)
        return records

    def _paper_payload(self, paper: Any) -> dict[str, Any]:
        return {
            "id": int(paper.id),
            "title": paper.title,
            "year": paper.publication_year,
            "publication_date": paper.publication_date,
            "abstract": self._truncate(paper.abstract),
            "cited_by_count": int(paper.cited_by_count or 0),
            "keywords": [
                keyword.value
                for keyword in getattr(paper, "keywords", [])
                if getattr(keyword, "value", None)
            ],
            "topics": [
                topic.name
                for topic in getattr(paper, "topics", [])
                if getattr(topic, "name", None)
            ],
        }

    def _generate_payload(
        self,
        *,
        topic: dict[str, Any],
        period: dict[str, Any],
        metrics: dict[str, Any],
        keyword_dynamics: dict[str, Any],
        openalex_counts: list[dict[str, Any]],
        representative_papers: list[dict[str, Any]],
        report_language: str,
    ) -> dict[str, Any]:
        content = self.chat_adapter.chat_completion(
            messages=self.prompt_service.build_report_prompt(
                topic=topic,
                period=period,
                metrics=metrics,
                keyword_dynamics=keyword_dynamics,
                openalex_counts=openalex_counts,
                representative_papers=representative_papers,
                report_language=report_language,
            ),
            model=self.chat_model,
            temperature=0.2,
            response_format=self.prompt_service.response_format(),
        )
        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            raise LLMGenerationError(
                "Topic quarter report response was expected to be JSON",
                details={"reason": str(exc), "content": content},
            ) from exc
        if not isinstance(payload, dict):
            raise LLMGenerationError(
                "Topic quarter report JSON must be an object",
                details={"content": content},
            )
        return payload

    def _items_from_payload(
        self,
        payload: dict[str, Any],
    ) -> list[TopicQuarterReportItemInputDTO]:
        raw_items = payload.get("items")
        if not isinstance(raw_items, list):
            return []
        items: list[TopicQuarterReportItemInputDTO] = []
        for index, item in enumerate(raw_items):
            if not isinstance(item, dict):
                continue
            item.setdefault("sort_order", index)
            try:
                items.append(TopicQuarterReportItemInputDTO.model_validate(item))
            except ValidationError:
                continue
        return items

    def _paper_links(
        self,
        representative_papers: list[dict[str, Any]],
    ) -> list[TopicQuarterReportPaperInputDTO]:
        links: list[TopicQuarterReportPaperInputDTO] = []
        for record in representative_papers:
            paper_id = int(record["id"])
            score = Decimal(str(record.get("score") or 0))
            for role in record.get("roles", []):
                if role not in {"representative", "highly_cited"}:
                    continue
                links.append(
                    TopicQuarterReportPaperInputDTO(
                        paper_id=paper_id,
                        role=role,
                        score=score,
                        note=record.get("title"),
                    )
                )
        return links

    def _weighted_average(
        self,
        rows: list[Any],
        field: str,
        *,
        weight_field: str,
    ) -> float | None:
        weighted_sum = 0.0
        weight_sum = 0
        for row in rows:
            value = getattr(row, field, None)
            weight = int(getattr(row, weight_field, 0) or 0)
            if value is None or weight <= 0:
                continue
            weighted_sum += float(value) * weight
            weight_sum += weight
        return weighted_sum / weight_sum if weight_sum else self._average(rows, field)

    def _average(self, rows: list[Any], field: str) -> float | None:
        values = [
            float(value)
            for value in (getattr(row, field, None) for row in rows)
            if value is not None
        ]
        return sum(values) / len(values) if values else None

    def _string_list(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        return [
            clean
            for clean in (self._clean_text(item) for item in value)
            if clean is not None
        ]

    def _int_list(self, value: Any) -> list[int]:
        if not isinstance(value, list):
            return []
        result: list[int] = []
        for item in value:
            try:
                result.append(int(item))
            except (TypeError, ValueError):
                continue
        return result

    def _clean_text(self, value: Any) -> str | None:
        if value is None:
            return None
        clean = " ".join(str(value).split())
        return clean or None

    def _truncate(self, value: str | None) -> str | None:
        if value is None:
            return None
        clean = self._clean_text(value)
        if clean is None:
            return None
        return clean[:ABSTRACT_CHAR_LIMIT]

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
                task_type="topic_quarter_report",
                entity_id=entity_id,
                stage=stage,
                current=current,
                total=total,
                message=message,
                payload=payload or {},
            )
        )


__all__ = ["TopicQuarterReportFacade"]
