from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from adapters.qdrant_adapter import QdrantAdapter
from adapters.redis_adapter import RedisAdapter
from core.exceptions import AppError, EntityNotFoundError, InvalidRequestError
from dto.common import BatchOperationResultDTO
from dto.qdrant import QdrantPointDTO
from dto.trends import TrendClusterDTO, TrendMetricsDTO
from ml.constants import PAPERS_COLLECTION, TREND_CLUSTERS_COLLECTION
from ml.facades.summaries import SummaryFacade
from ml.services.events import EventSink, MLEvent, NoopEventSink
from ml.services.qdrant_payloads import QdrantPayloadBuilder
from ml.services.scoring import ScoringService
from ml.services.trend_status import TrendStatusService
from ml.services.vector_math import VectorMathService
from repositories.graph import PaperGraphRepository
from repositories.papers import PaperRepository
from repositories.research_clusters import ResearchClusterRepository
from repositories.taxonomy import TaxonomyRepository

ENTITY_PAGE_SIZE = 500
QDRANT_VECTOR_RETRIEVE_BATCH_SIZE = 256
PAPER_METADATA_BATCH_SIZE = 1000
MIN_INDEXED_PAPERS_FOR_CLUSTER = 2
REPRESENTATIVE_PAPER_LIMIT = 5
REPRESENTATIVE_CANDIDATE_LIMIT = 2000
TOP_KEYWORD_LIMIT = 10
CLUSTER_CACHE_INDEX_KEY = "ml:trend_clusters:index"
CLUSTER_CACHE_KEY_PREFIX = "ml:trend_cluster:"
CLUSTER_CACHE_TTL_SECONDS = 24 * 60 * 60


@dataclass
class _ClusterVectorData:
    centroid: list[float]
    indexed_paper_count: int
    representative_candidates: list[QdrantPointDTO]
    top_keywords: list[str]


@dataclass
class _PaperMetadata:
    paper_dates: list[date]
    citation_count_sum: int
    avg_cited_by_count: Decimal | None
    representative_paper_by_id: dict[int, Any]


class ClusterAnalyticsFacade:
    """Facade for recomputing topic-based trend clusters."""

    def __init__(
        self,
        *,
        taxonomy_repository: TaxonomyRepository,
        paper_repository: PaperRepository,
        paper_graph_repository: PaperGraphRepository,
        qdrant_adapter: QdrantAdapter,
        redis_adapter: RedisAdapter,
        summary_facade: SummaryFacade,
        research_cluster_repository: ResearchClusterRepository | None = None,
        vector_math_service: VectorMathService | None = None,
        scoring_service: ScoringService | None = None,
        trend_status_service: TrendStatusService | None = None,
        payload_builder: QdrantPayloadBuilder | None = None,
        event_sink: EventSink | None = None,
        papers_collection: str = PAPERS_COLLECTION,
        trend_clusters_collection: str = TREND_CLUSTERS_COLLECTION,
    ) -> None:
        self.taxonomy_repository = taxonomy_repository
        self.paper_repository = paper_repository
        self.paper_graph_repository = paper_graph_repository
        self.qdrant_adapter = qdrant_adapter
        self.redis_adapter = redis_adapter
        self.summary_facade = summary_facade
        self.research_cluster_repository = research_cluster_repository
        self.vector_math_service = vector_math_service or VectorMathService()
        self.scoring_service = scoring_service or ScoringService()
        self.trend_status_service = trend_status_service or TrendStatusService()
        self.payload_builder = payload_builder or QdrantPayloadBuilder()
        self.event_sink = event_sink or NoopEventSink()
        self.papers_collection = papers_collection
        self.trend_clusters_collection = trend_clusters_collection

    def recompute_all_clusters(
        self,
        date_from: date | None = None,
        date_to: date | None = None,
        limit: int | None = None,
        force_summary: bool = False,
        batch_size: int = ENTITY_PAGE_SIZE,
    ) -> BatchOperationResultDTO:
        """Recompute trend clusters for topics selected from PostgreSQL.

        When ``date_from`` and ``date_to`` are supplied, topics are ordered by
        recent growth in that period. Otherwise topics are read in pages from the
        taxonomy repository. Each cluster is recomputed through
        ``recompute_cluster`` and written to ``trend_clusters_v1``.
        """
        if limit is not None and limit < 0:
            raise InvalidRequestError(
                "limit must be non-negative",
                details={"limit": limit},
            )
        if batch_size <= 0:
            raise InvalidRequestError(
                "batch_size must be positive",
                details={"batch_size": batch_size},
            )
        if date_from is not None and date_to is not None and date_from > date_to:
            raise InvalidRequestError(
                "date_from must be less than or equal to date_to",
                details={"date_from": date_from, "date_to": date_to},
            )

        result = BatchOperationResultDTO()
        self._emit(
            "cluster_batch_started",
            entity_id="all",
            stage="batch",
            current=0,
            total=limit,
            message="Starting trend cluster recompute",
            payload={
                "date_from": date_from,
                "date_to": date_to,
                "limit": limit,
                "batch_size": batch_size,
            },
        )
        for topic_id in self._iter_topic_ids(date_from, date_to, limit, batch_size):
            result.total += 1
            try:
                self.recompute_cluster(
                    f"topic:{topic_id}",
                    force_summary=force_summary,
                )
            except InvalidRequestError as exc:
                if self._is_skip_error(exc):
                    result.skipped += 1
                else:
                    result.failed += 1
                    result.errors.append(self._error_payload(topic_id, exc))
            except AppError as exc:
                result.failed += 1
                result.errors.append(self._error_payload(topic_id, exc))
            else:
                result.updated += 1
            self._emit(
                "cluster_batch_progress",
                entity_id="all",
                stage="topics",
                current=result.total,
                total=limit,
                message=(
                    f"topics done={result.total} updated={result.updated} "
                    f"skipped={result.skipped} failed={result.failed}"
                ),
            )
        self._emit(
            "cluster_batch_completed",
            entity_id="all",
            stage="topics",
            current=result.total,
            total=result.total,
            message=(
                f"Cluster recompute completed: updated={result.updated} "
                f"skipped={result.skipped} failed={result.failed}"
            ),
            payload=result.model_dump(mode="json"),
        )
        return result

    def recompute_cluster(
        self,
        cluster_id: str,
        force_summary: bool = False,
    ) -> TrendClusterDTO:
        """Recompute one MVP trend cluster identified as ``topic:{topic_id}``."""
        self._emit(
            "cluster_started",
            entity_id=cluster_id,
            stage="started",
            message="Starting cluster recompute",
            payload={"force_summary": force_summary},
        )
        try:
            topic_id = self._parse_topic_cluster_id(cluster_id)
            topic = self.taxonomy_repository.get_topic_by_id(topic_id)
            if topic is None:
                raise EntityNotFoundError(
                    "Topic not found",
                    details={"cluster_id": cluster_id, "topic_id": topic_id},
                )

            paper_ids = self.taxonomy_repository.list_paper_ids_by_topic(topic_id)
            self._emit(
                "paper_ids_loaded",
                entity_id=cluster_id,
                stage="paper_ids",
                current=len(paper_ids),
                total=len(paper_ids),
                message=f"Loaded {len(paper_ids)} topic paper ids",
            )
            vector_data = self._cluster_vector_data(paper_ids, cluster_id=cluster_id)
            if vector_data.indexed_paper_count < MIN_INDEXED_PAPERS_FOR_CLUSTER:
                raise InvalidRequestError(
                    "Topic has fewer than two indexed papers",
                    details={
                        "cluster_id": cluster_id,
                        "topic_id": topic_id,
                        "indexed_paper_count": vector_data.indexed_paper_count,
                        "reason": "insufficient_indexed_papers",
                    },
                )

            centroid = vector_data.centroid
            representative_paper_ids = self._representative_paper_ids(
                vector_data.representative_candidates,
                centroid,
            )
            paper_metadata = self._paper_metadata(
                paper_ids,
                representative_paper_ids,
                cluster_id=cluster_id,
            )

            metrics_payload = self._calculate_metrics(paper_metadata.paper_dates)
            self._emit(
                "metrics_calculated",
                entity_id=cluster_id,
                stage="metrics",
                message=(
                    f"trend_score={metrics_payload['trend_score']:.3f} "
                    f"status={metrics_payload['status']}"
                ),
                payload=metrics_payload,
            )
            top_keywords = vector_data.top_keywords
            representative_titles = self._paper_titles(
                representative_paper_ids,
                paper_metadata.representative_paper_by_id,
            )
            representative_abstracts = self._paper_abstracts(
                representative_paper_ids,
                paper_metadata.representative_paper_by_id,
            )

            existing_cluster = self.get_cluster(cluster_id)
            summary = existing_cluster.summary if existing_cluster is not None else None
            cluster_name = (
                existing_cluster.name
                if existing_cluster is not None and existing_cluster.name
                else self._topic_name(topic)
            )
            summary_degraded = False
            if force_summary or not summary:
                self._emit(
                    "summary_started",
                    entity_id=cluster_id,
                    stage="summary",
                    message="Generating cluster summary",
                )
                cluster_summary = self.summary_facade.summarize_cluster(
                    cluster_name=self._topic_name(topic),
                    paper_titles=representative_titles,
                    abstracts=representative_abstracts,
                    top_keywords=top_keywords,
                )
                summary = cluster_summary.summary
                summary_degraded = cluster_summary.degraded
                if not summary_degraded:
                    cluster_name = cluster_summary.title
                self._emit(
                    "summary_completed",
                    entity_id=cluster_id,
                    stage="summary",
                    message=(
                        "Cluster summary generated"
                        if not summary_degraded
                        else "Cluster summary generated with fallback"
                    ),
                    payload={"degraded": summary_degraded},
                )

            trend_cluster = TrendClusterDTO(
                id=topic_id,
                cluster_key=cluster_id,
                cluster_type="topic",
                name=cluster_name,
                summary=summary,
                status=metrics_payload["status"],
                source_topic_id=topic_id,
                metrics=TrendMetricsDTO(
                    paper_count=metrics_payload["paper_count_total"],
                    previous_paper_count=metrics_payload["previous_90d_count"],
                    growth_rate=self._decimal(metrics_payload["growth_rate_90d"]),
                    trend_score=self._decimal(metrics_payload["trend_score"]),
                    citation_count_sum=paper_metadata.citation_count_sum,
                    avg_cited_by_count=paper_metadata.avg_cited_by_count,
                ),
                top_keywords=top_keywords,
                representative_paper_ids=representative_paper_ids,
            )

            payload = self.payload_builder.build_trend_cluster_payload(
                trend_cluster,
                cluster_id=cluster_id,
                **metrics_payload,
                source_topic_name=self._topic_name(topic),
                indexed_paper_count=vector_data.indexed_paper_count,
                vector_retrieve_batch_size=QDRANT_VECTOR_RETRIEVE_BATCH_SIZE,
                representative_candidate_count=len(
                    vector_data.representative_candidates
                ),
                summary_degraded=summary_degraded,
                indexed_at=datetime.now(timezone.utc),
            )
            self.qdrant_adapter.upsert_point(
                self.trend_clusters_collection,
                cluster_id,
                centroid,
                payload,
            )
            self._emit(
                "qdrant_upsert_completed",
                entity_id=cluster_id,
                stage="qdrant_upsert",
                message="Cluster point upserted",
                payload={"collection": self.trend_clusters_collection},
            )
            if self.research_cluster_repository is not None:
                self._emit(
                    "db_cluster_upsert_started",
                    entity_id=cluster_id,
                    stage="db_upsert",
                    message="Upserting cluster row",
                )
                try:
                    self.research_cluster_repository.upsert_cluster_from_payload(
                        payload
                    )
                except Exception as exc:
                    self._emit(
                        "db_cluster_upsert_failed",
                        entity_id=cluster_id,
                        stage="db_upsert",
                        message=str(exc),
                        payload={"error_type": exc.__class__.__name__},
                    )
                    raise
                self._emit(
                    "db_cluster_upsert_completed",
                    entity_id=cluster_id,
                    stage="db_upsert",
                    message="Cluster row upserted",
                )
            self._cache_cluster(trend_cluster)
            self._emit(
                "cluster_completed",
                entity_id=cluster_id,
                stage="completed",
                message="Cluster recompute completed",
                payload={
                    "indexed_paper_count": vector_data.indexed_paper_count,
                    "trend_score": metrics_payload["trend_score"],
                    "status": metrics_payload["status"],
                },
            )
            return trend_cluster
        except Exception as exc:
            self._emit(
                "cluster_failed",
                entity_id=cluster_id,
                stage="failed",
                message=str(exc),
                payload={"error_type": exc.__class__.__name__},
            )
            raise

    def get_cluster(
        self,
        cluster_id: str,
    ) -> TrendClusterDTO | None:
        points = self.qdrant_adapter.retrieve(
            self.trend_clusters_collection,
            [cluster_id],
            with_vectors=False,
        )
        if points:
            return self._cluster_from_payload(cluster_id, points[0].payload)
        cached = self.redis_adapter.get_json(self._cluster_cache_key(cluster_id))
        if cached is None:
            return None
        return TrendClusterDTO.model_validate(cached["cluster"])

    def get_top_clusters(
        self,
        limit: int = 20,
        status: str | None = None,
    ) -> list[TrendClusterDTO]:
        if limit < 1:
            raise InvalidRequestError(
                "limit must be positive",
                details={"limit": limit},
            )
        index = self.redis_adapter.get_json(CLUSTER_CACHE_INDEX_KEY) or {}
        cluster_ids = index.get("cluster_ids", [])
        clusters: list[TrendClusterDTO] = []
        for cluster_id in cluster_ids:
            cluster = self.get_cluster(str(cluster_id))
            if cluster is None:
                continue
            if status is not None and cluster.status != status:
                continue
            clusters.append(cluster)

        clusters.sort(
            key=lambda cluster: float(cluster.metrics.trend_score or Decimal("0")),
            reverse=True,
        )
        return clusters[:limit]

    def _iter_topic_ids(
        self,
        date_from: date | None,
        date_to: date | None,
        limit: int | None,
        batch_size: int,
    ):
        if date_from is not None and date_to is not None:
            for item in self.paper_graph_repository.get_top_topics_by_recent_growth(
                date_from,
                date_to,
                limit or batch_size,
            ):
                yield int(item["topic_id"])
            return

        emitted = 0
        offset = 0
        while limit is None or emitted < limit:
            page_limit = batch_size
            if limit is not None:
                page_limit = min(page_limit, limit - emitted)
            if page_limit <= 0:
                break
            topics = self.taxonomy_repository.list_topics(page_limit, offset)
            if not topics:
                break
            for topic in topics:
                yield int(getattr(topic, "id"))
                emitted += 1
                if limit is not None and emitted >= limit:
                    return
            if len(topics) < page_limit:
                break
            offset += page_limit

    def _cluster_vector_data(
        self,
        paper_ids: list[int],
        *,
        cluster_id: str | None = None,
    ) -> _ClusterVectorData:
        mean_vector: list[float] | None = None
        indexed_paper_count = 0
        representative_candidates: list[QdrantPointDTO] = []
        keyword_counter: Counter[str] = Counter()
        processed = 0

        for chunk in self._chunks(paper_ids, QDRANT_VECTOR_RETRIEVE_BATCH_SIZE):
            points = self.qdrant_adapter.retrieve(
                self.papers_collection,
                chunk,
                with_vectors=True,
            )
            processed += len(chunk)
            for point in self._indexed_points(points):
                mean_vector, indexed_paper_count = (
                    self.vector_math_service.update_mean_vector(
                        mean_vector,
                        indexed_paper_count,
                        point.vector,
                    )
                )
                if len(representative_candidates) < REPRESENTATIVE_CANDIDATE_LIMIT:
                    representative_candidates.append(point)
                self._add_payload_keywords(keyword_counter, point)
            self._emit(
                "qdrant_vectors_progress",
                entity_id=cluster_id,
                stage="qdrant_vectors",
                current=processed,
                total=len(paper_ids),
                message=f"vectors={indexed_paper_count}",
                payload={"indexed_paper_count": indexed_paper_count},
            )

        return _ClusterVectorData(
            centroid=mean_vector or [],
            indexed_paper_count=indexed_paper_count,
            representative_candidates=representative_candidates,
            top_keywords=[
                keyword for keyword, _ in keyword_counter.most_common(TOP_KEYWORD_LIMIT)
            ],
        )

    def _paper_metadata(
        self,
        paper_ids: list[int],
        representative_paper_ids: list[int],
        *,
        cluster_id: str | None = None,
    ) -> _PaperMetadata:
        paper_dates: list[date] = []
        citation_count_sum = 0
        paper_count = 0
        representative_id_set = set(representative_paper_ids)
        representative_paper_by_id: dict[int, Any] = {}

        processed = 0
        for chunk in self._chunks(paper_ids, PAPER_METADATA_BATCH_SIZE):
            for paper in self.paper_repository.get_by_ids(chunk):
                paper_count += 1
                paper_id = getattr(paper, "id", None)
                if paper_id in representative_id_set:
                    representative_paper_by_id[int(paper_id)] = paper
                citation_count_sum += int(getattr(paper, "cited_by_count", 0) or 0)
                paper_dates.extend(self._paper_dates([paper]))
            processed += len(chunk)
            self._emit(
                "postgres_metadata_progress",
                entity_id=cluster_id,
                stage="postgres_metadata",
                current=processed,
                total=len(paper_ids),
                message=f"metadata={paper_count}",
                payload={"loaded_paper_count": paper_count},
            )

        avg_cited_by_count = (
            self._decimal(citation_count_sum / paper_count) if paper_count else None
        )
        return _PaperMetadata(
            paper_dates=paper_dates,
            citation_count_sum=citation_count_sum,
            avg_cited_by_count=avg_cited_by_count,
            representative_paper_by_id=representative_paper_by_id,
        )

    def _calculate_metrics(self, paper_dates: list[date]) -> dict[str, Any]:
        today = date.today()
        paper_count_total = len(paper_dates)
        paper_count_30d = self._count_between(
            paper_dates,
            today - timedelta(days=30),
            today,
        )
        previous_30d_count = self._count_between(
            paper_dates,
            today - timedelta(days=60),
            today - timedelta(days=31),
        )
        paper_count_90d = self._count_between(
            paper_dates,
            today - timedelta(days=90),
            today,
        )
        previous_90d_count = self._count_between(
            paper_dates,
            today - timedelta(days=180),
            today - timedelta(days=91),
        )
        growth_rate_30d = self.scoring_service.calculate_growth_rate(
            paper_count_30d,
            previous_30d_count,
        )
        growth_rate_90d = self.scoring_service.calculate_growth_rate(
            paper_count_90d,
            previous_90d_count,
        )
        acceleration = self.scoring_service.calculate_acceleration(
            growth_rate_30d,
            growth_rate_90d,
        )
        trend_score = self.scoring_service.calculate_trend_score(
            growth_rate=growth_rate_90d,
            recent_paper_count=paper_count_90d,
            acceleration=acceleration,
        )
        age_periods = self._age_periods(paper_dates)
        status = self.trend_status_service.determine_status(
            age_periods=age_periods,
            recent_paper_count=paper_count_90d,
            growth_rate=growth_rate_90d,
        )
        return {
            "paper_count_total": paper_count_total,
            "paper_count_30d": paper_count_30d,
            "paper_count_90d": paper_count_90d,
            "previous_30d_count": previous_30d_count,
            "previous_90d_count": previous_90d_count,
            "growth_rate_30d": growth_rate_30d,
            "growth_rate_90d": growth_rate_90d,
            "acceleration": acceleration,
            "trend_score": trend_score,
            "status": status,
        }

    def _representative_paper_ids(
        self,
        points: list[QdrantPointDTO],
        centroid: list[float],
    ) -> list[int]:
        scored_points = [
            (
                self._paper_id_from_point(point),
                self.vector_math_service.cosine_similarity(point.vector, centroid),
            )
            for point in points
        ]
        scored_points.sort(key=lambda item: item[1], reverse=True)
        return [
            paper_id
            for paper_id, _ in scored_points[:REPRESENTATIVE_PAPER_LIMIT]
            if paper_id is not None
        ]

    def _top_keywords(self, points: list[QdrantPointDTO]) -> list[str]:
        counter: Counter[str] = Counter()
        for point in points:
            self._add_payload_keywords(counter, point)
        if counter:
            return [keyword for keyword, _ in counter.most_common(TOP_KEYWORD_LIMIT)]

        for point in points:
            paper_id = self._paper_id_from_point(point)
            if paper_id is not None:
                try:
                    keywords = self.taxonomy_repository.list_keywords_by_paper(paper_id)
                except Exception:
                    keywords = []
                for keyword in keywords:
                    value = getattr(keyword, "value", None)
                    if value:
                        counter[str(value)] += 1
        return [keyword for keyword, _ in counter.most_common(TOP_KEYWORD_LIMIT)]

    def _add_payload_keywords(
        self,
        counter: Counter[str],
        point: QdrantPointDTO,
    ) -> None:
        keywords = point.payload.get("keywords", [])
        if not isinstance(keywords, list):
            return
        for value in keywords:
            if value:
                counter[str(value)] += 1

    def _cluster_from_payload(
        self,
        cluster_id: str,
        payload: dict[str, Any],
    ) -> TrendClusterDTO:
        topic_id = int(
            payload.get("source_topic_id") or self._parse_topic_cluster_id(cluster_id)
        )
        return TrendClusterDTO(
            id=topic_id,
            cluster_key=str(payload.get("cluster_key") or cluster_id),
            cluster_type=str(payload.get("cluster_type") or "topic"),
            name=str(payload.get("name") or cluster_id),
            summary=payload.get("summary"),
            status=payload.get("status"),
            source_topic_id=topic_id,
            metrics=TrendMetricsDTO(
                paper_count=int(
                    payload.get("paper_count_total") or payload.get("paper_count") or 0
                ),
                previous_paper_count=int(
                    payload.get("previous_90d_count")
                    or payload.get("previous_paper_count")
                    or 0
                ),
                growth_rate=self._decimal_or_none(
                    payload.get("growth_rate_90d") or payload.get("growth_rate")
                ),
                trend_score=self._decimal_or_none(payload.get("trend_score")),
                semantic_drift=self._decimal_or_none(payload.get("semantic_drift")),
                citation_count_sum=payload.get("citation_count_sum"),
                avg_cited_by_count=self._decimal_or_none(
                    payload.get("avg_cited_by_count")
                ),
            ),
            top_keywords=[str(value) for value in payload.get("top_keywords", [])],
            representative_paper_ids=[
                int(value) for value in payload.get("representative_paper_ids", [])
            ],
        )

    def _cache_cluster(self, cluster: TrendClusterDTO) -> None:
        cluster_id = cluster.cluster_key
        self.redis_adapter.set_json(
            self._cluster_cache_key(cluster_id),
            {"cluster": cluster.model_dump(mode="json")},
            ttl_seconds=CLUSTER_CACHE_TTL_SECONDS,
        )
        index = self.redis_adapter.get_json(CLUSTER_CACHE_INDEX_KEY) or {}
        cluster_ids = [str(value) for value in index.get("cluster_ids", [])]
        if cluster_id not in cluster_ids:
            cluster_ids.append(cluster_id)
        self.redis_adapter.set_json(
            CLUSTER_CACHE_INDEX_KEY,
            {"cluster_ids": cluster_ids},
            ttl_seconds=CLUSTER_CACHE_TTL_SECONDS,
        )

    def _indexed_points(self, points: list[QdrantPointDTO]) -> list[QdrantPointDTO]:
        return [point for point in points if point.vector]

    def _paper_dates(self, papers: list[Any]) -> list[date]:
        result: list[date] = []
        for paper in papers:
            publication_date = getattr(paper, "publication_date", None)
            if isinstance(publication_date, datetime):
                result.append(publication_date.date())
            elif isinstance(publication_date, date):
                result.append(publication_date)
        return result

    def _count_between(
        self,
        dates: list[date],
        start: date,
        end: date,
    ) -> int:
        return sum(1 for value in dates if start <= value <= end)

    def _age_periods(self, dates: list[date]) -> int:
        if not dates:
            return 0
        age_days = max(0, (date.today() - min(dates)).days)
        return age_days // 30 + 1

    def _paper_titles(
        self,
        paper_ids: list[int],
        paper_by_id: dict[int, Any],
    ) -> list[str]:
        return [
            str(getattr(paper_by_id[paper_id], "title"))
            for paper_id in paper_ids
            if paper_id in paper_by_id and getattr(paper_by_id[paper_id], "title", None)
        ]

    def _paper_abstracts(
        self,
        paper_ids: list[int],
        paper_by_id: dict[int, Any],
    ) -> list[str]:
        return [
            str(getattr(paper_by_id[paper_id], "abstract"))
            for paper_id in paper_ids
            if paper_id in paper_by_id
            and getattr(paper_by_id[paper_id], "abstract", None)
        ]

    def _citation_count_sum(self, papers: list[Any]) -> int:
        return sum(int(getattr(paper, "cited_by_count", 0) or 0) for paper in papers)

    def _avg_cited_by_count(self, papers: list[Any]) -> Decimal | None:
        if not papers:
            return None
        return self._decimal(self._citation_count_sum(papers) / len(papers))

    def _chunks(self, values: list[int], size: int):
        for index in range(0, len(values), size):
            yield values[index : index + size]

    def _paper_id_from_point(self, point: QdrantPointDTO) -> int | None:
        paper_id = point.payload.get("paper_id") or point.id
        try:
            return int(paper_id)
        except (TypeError, ValueError):
            return None

    def _topic_name(self, topic: Any) -> str:
        name = getattr(topic, "name", None)
        if not name:
            raise InvalidRequestError(
                "Topic name is required for cluster analytics",
                details={"topic_id": getattr(topic, "id", None)},
            )
        return str(name)

    def _parse_topic_cluster_id(self, cluster_id: str) -> int:
        prefix = "topic:"
        if not cluster_id.startswith(prefix):
            raise InvalidRequestError(
                "Unsupported cluster_id format",
                details={"cluster_id": cluster_id, "expected": "topic:{topic_id}"},
            )
        try:
            return int(cluster_id.removeprefix(prefix))
        except ValueError as exc:
            raise InvalidRequestError(
                "Invalid topic cluster id",
                details={"cluster_id": cluster_id},
            ) from exc

    def _cluster_cache_key(self, cluster_id: str) -> str:
        return f"{CLUSTER_CACHE_KEY_PREFIX}{cluster_id}"

    def _is_skip_error(self, exc: InvalidRequestError) -> bool:
        details = exc.details or {}
        return details.get("reason") in {
            "insufficient_indexed_papers",
            "no_indexed_vectors",
        }

    def _error_payload(self, topic_id: int, exc: AppError) -> dict[str, Any]:
        return {
            "cluster_id": f"topic:{topic_id}",
            "topic_id": topic_id,
            "code": exc.code,
            "message": exc.message,
            "details": exc.details or {},
        }

    def _decimal(self, value: float | int) -> Decimal:
        return Decimal(str(value))

    def _decimal_or_none(self, value: Any) -> Decimal | None:
        if value is None:
            return None
        return Decimal(str(value))

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
                task_type="cluster_recompute",
                entity_id=entity_id,
                stage=stage,
                current=current,
                total=total,
                message=message,
                payload=payload or {},
            )
        )


__all__ = ["ClusterAnalyticsFacade"]
