from __future__ import annotations

from .cluster_dynamics_pipeline import ClusterDynamicsPipeline
from .keyword_extraction_pipeline import KeywordExtractionPipeline
from .openalex_paper_loading_pipeline import OpenAlexPaperLoadingPipeline
from .paper_indexing_pipeline import PaperIndexingPipeline
from .recommendation_pipeline import RecommendationPipeline
from .research_entities_pipeline import ResearchEntitiesPipeline
from .trend_recompute_pipeline import TrendRecomputePipeline
from .user_profile_pipeline import UserProfilePipeline

# TODO: а Pipeline в рамках проекта вообще нужны?

__all__ = [
    "ClusterDynamicsPipeline",
    "KeywordExtractionPipeline",
    "OpenAlexPaperLoadingPipeline",
    "PaperIndexingPipeline",
    "RecommendationPipeline",
    "ResearchEntitiesPipeline",
    "TrendRecomputePipeline",
    "UserProfilePipeline",
]
