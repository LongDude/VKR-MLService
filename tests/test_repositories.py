from __future__ import annotations

from datetime import date
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from core.config import Settings
from dto.charts import PeriodCountDTO
from models import Paper, TopicQuarterReport
from models.models import User
from models.session import create_engine_from_settings, create_session_factory
from repositories import (
    AuthorRepository,
    BaseRepository,
    FavouriteRepository,
    InstitutionRepository,
    LandingRepository,
    PaperGraphRepository,
    PaperRepository,
    TaxonomyRepository,
    TopicQuarterReportRepository,
    TrackedAreaRepository,
    UserRepository,
)


REPOSITORY_METHODS = {
    UserRepository: [
        "get_by_id",
        "get_by_email",
        "exists_by_email",
        "create",
        "update_name",
    ],
    PaperRepository: [
        "get_by_id",
        "get_by_ids",
        "get_by_doi",
        "get_by_openalex_id",
        "get_by_title_normalized",
        "create",
        "update",
        "upsert_from_external",
        "list_by_period",
        "list_recent",
        "get_indexed_text_hashes",
        "mark_loaded",
        "mark_indexing_started",
        "mark_indexed",
        "mark_failed",
    ],
    AuthorRepository: [
        "get_by_id",
        "get_by_orcid",
        "get_or_create",
        "attach_to_paper",
        "list_by_paper",
    ],
    InstitutionRepository: [
        "get_by_id",
        "get_by_ror",
        "get_or_create",
        "attach_to_author",
        "list_by_author",
    ],
    LandingRepository: [
        "list_by_paper",
        "get_best_by_paper",
        "upsert",
        "set_best",
    ],
    TaxonomyRepository: [
        "get_domain_by_id",
        "get_field_by_id",
        "get_subfield_by_id",
        "get_topic_by_id",
        "get_keyword_by_id",
        "get_or_create_domain",
        "get_or_create_field",
        "get_or_create_subfield",
        "get_or_create_topic",
        "get_or_create_keyword",
        "attach_topic_to_paper",
        "attach_keyword_to_paper",
        "list_topics_by_paper",
        "list_keywords_by_paper",
        "list_paper_ids_by_topic",
        "list_domains",
        "list_fields",
        "list_subfields",
        "list_topics",
        "list_keywords",
    ],
    FavouriteRepository: [
        "add",
        "remove",
        "exists",
        "list_paper_ids",
        "list_papers",
        "count",
    ],
    TrackedAreaRepository: [
        "add_domain",
        "remove_domain",
        "add_field",
        "remove_field",
        "add_subfield",
        "remove_subfield",
        "add_topic",
        "remove_topic",
        "add_keyword",
        "remove_keyword",
        "list_domain_ids",
        "list_field_ids",
        "list_subfield_ids",
        "list_topic_ids",
        "list_keyword_ids",
        "get_user_profile_source",
    ],
    PaperGraphRepository: [
        "count_papers_by_topic_and_period",
        "count_papers_by_topics_heatmap",
        "get_top_topics_by_recent_growth",
    ],
    TopicQuarterReportRepository: [
        "get_by_topic_period",
        "upsert_report",
        "replace_items",
        "replace_papers",
        "list_existing_keys",
        "to_dto",
    ],
}


def test_models_models_reexports_orm_classes() -> None:
    assert User.__tablename__ == "users"
    assert Paper.__tablename__ == "papers"


def test_session_helpers_create_sync_session_factory() -> None:
    engine = create_engine_from_settings(Settings(database_url="sqlite+pysqlite:///:memory:"))
    session_factory = create_session_factory(engine)

    assert session_factory.kw["bind"] is engine


def test_period_count_dto_exists_for_graph_repository() -> None:
    dto = PeriodCountDTO(period_start="2026-01-01", count=3)

    assert dto.count == 3


def test_repository_classes_expose_required_docstring_methods() -> None:
    assert BaseRepository.__init__.__doc__
    for repository_type, method_names in REPOSITORY_METHODS.items():
        for method_name in method_names:
            method = getattr(repository_type, method_name)
            assert method.__doc__, f"{repository_type.__name__}.{method_name} lacks docstring"


class _FakeDialect:
    name = "sqlite"


class _FakeBind:
    dialect = _FakeDialect()


class _FakeTopicQuarterReportSession:
    def __init__(self, existing: TopicQuarterReport | None = None) -> None:
        self.existing = existing
        self.added: list[TopicQuarterReport] = []
        self.flushes = 0

    def get_bind(self) -> _FakeBind:
        return _FakeBind()

    def scalar(self, _stmt: object) -> TopicQuarterReport | None:
        return self.existing

    def add(self, instance: TopicQuarterReport) -> None:
        if getattr(instance, "id", None) is None:
            instance.id = 1
        self.added.append(instance)

    def flush(self) -> None:
        self.flushes += 1


def test_topic_quarter_report_repository_uses_period_characterization() -> None:
    session = _FakeTopicQuarterReportSession()
    repository = TopicQuarterReportRepository(session)  # type: ignore[arg-type]

    report, created = repository.upsert_report(
        topic_id=1,
        period_start=date(2025, 1, 1),
        period_end=date(2025, 3, 31),
        period_key="2025-Q1",
        summary="timeline summary",
        period_characterization="quarter state",
        dynamics_summary="dynamic",
        future_dynamics="future",
        metrics={"paper_count": 3},
        keyword_dynamics={"top_keywords": ["graph"]},
    )
    dto = repository.to_dto(report)

    assert created is True
    assert not hasattr(report, "title")
    assert not hasattr(report, "definition")
    assert report.period_characterization == "quarter state"
    assert dto.period_characterization == "quarter state"
    assert not hasattr(dto, "title")
    assert not hasattr(dto, "definition")


def test_topic_quarter_report_repository_updates_period_characterization() -> None:
    existing = TopicQuarterReport(
        id=7,
        topic_id=1,
        period_start=date(2025, 1, 1),
        period_end=date(2025, 3, 31),
        period_key="2025-Q1",
        summary="old",
        period_characterization="old state",
        dynamics_summary="old dynamic",
        future_dynamics="old future",
        metrics={},
        keyword_dynamics={},
    )
    session = _FakeTopicQuarterReportSession(existing=existing)
    repository = TopicQuarterReportRepository(session)  # type: ignore[arg-type]

    report, created = repository.upsert_report(
        topic_id=1,
        period_start=date(2025, 1, 1),
        period_end=date(2025, 3, 31),
        period_key="2025-Q1",
        summary="new",
        period_characterization="new state",
        dynamics_summary="new dynamic",
        future_dynamics="new future",
        metrics={"paper_count": 5},
        keyword_dynamics={"top_keywords": ["matching"]},
    )

    assert created is False
    assert report is existing
    assert report.period_characterization == "new state"
    assert report.summary == "new"
    assert session.added == []
