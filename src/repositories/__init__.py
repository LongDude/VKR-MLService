from __future__ import annotations

from .authors import AuthorRepository
from .base import BaseRepository
from .favourites import FavouriteRepository
from .graph import PaperGraphRepository
from .institutions import InstitutionRepository
from .landings import LandingRepository
from .paper_meta_sources import PaperMetaSourceRepository
from .paper_processing_states import PaperProcessingStateRepository
from .papers import PaperRepository
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
    "PaperGraphRepository",
    "PaperMetaSourceRepository",
    "PaperProcessingStateRepository",
    "PaperRepository",
    "ResearchClusterRepository",
    "TaxonomyRepository",
    "TrackedAreaRepository",
    "UserRepository",
]
