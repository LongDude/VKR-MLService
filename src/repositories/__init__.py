from __future__ import annotations

from .authors import AuthorRepository
from .base import BaseRepository
from .favourites import FavouriteRepository
from .graph import PaperGraphRepository
from .institutions import InstitutionRepository
from .landings import LandingRepository
from .papers import PaperRepository
from .openalex_topic_stats import OpenAlexTopicMonthlyCount, OpenAlexTopicStatsRepository
from .openalex_yearly_topic_stats import (
    OpenAlexTopicYearlyArtificialEstimate,
    OpenAlexTopicYearlyCount,
    OpenAlexYearlyTopicStatsRepository,
)
from .research_clusters import ResearchClusterRepository
from .taxonomy import TaxonomyRepository
from .tracked_areas import TrackedAreaRepository
from .users import UserRepository

__all__ = [
    "AuthorRepository",
    "BaseRepository",
    "FavouriteRepository",
    "InstitutionRepository",
    "LandingRepository",
    "OpenAlexTopicMonthlyCount",
    "OpenAlexTopicStatsRepository",
    "OpenAlexTopicYearlyArtificialEstimate",
    "OpenAlexTopicYearlyCount",
    "OpenAlexYearlyTopicStatsRepository",
    "PaperGraphRepository",
    "PaperRepository",
    "ResearchClusterRepository",
    "TaxonomyRepository",
    "TrackedAreaRepository",
    "UserRepository",
]
