from __future__ import annotations

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import models
from models import Base
from sqlalchemy.orm import configure_mappers


def test_all_migration_tables_are_registered() -> None:
    expected_tables = {
        "users",
        "papers",
        "user_favourite_papers",
        "authors",
        "institutions",
        "paper_authors",
        "author_institutions",
        "landings",
        "domains",
        "fields",
        "subfields",
        "topics",
        "paper_topics",
        "keywords",
        "paper_keywords",
        "user_tracked_domains",
        "user_tracked_fields",
        "user_tracked_keywords",
        "user_tracked_topics",
        "user_tracked_subfields",
        "research_clusters",
        "research_cluster_period_stats",
        "openalex_montly_topic_stats",
        "openalex_yearly_topic_stats",
        "topic_quarter_reports",
        "topic_quarter_report_items",
        "topic_quarter_report_papers",
    }

    assert models is not None
    assert expected_tables == set(Base.metadata.tables)


def test_sqlalchemy_mappers_are_configurable() -> None:
    configure_mappers()
