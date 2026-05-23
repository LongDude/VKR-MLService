from __future__ import annotations

import importlib
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


MODULES = [
    "core.config",
    "core.exceptions",
    "core.logging",
    "models",
    "models.analytics",
    "models.associations",
    "models.author",
    "models.base",
    "models.institution",
    "models.keyword",
    "models.models",
    "models.paper",
    "models.session",
    "models.topic",
    "models.user",
    "dto.common",
    "dto.errors",
    "dto.papers",
    "dto.authors",
    "dto.institutions",
    "dto.taxonomy",
    "dto.favourites",
    "dto.tracked",
    "dto.recommendations",
    "dto.trends",
    "dto.charts",
    "dto.embeddings",
    "dto.qdrant",
    "dto.external",
    "repositories.base",
    "repositories.users",
    "repositories.papers",
    "repositories.authors",
    "repositories.institutions",
    "repositories.landings",
    "repositories.taxonomy",
    "repositories.favourites",
    "repositories.tracked_areas",
    "repositories.graph",
    "adapters.redis_adapter",
    "adapters.qdrant_adapter",
    "adapters.lmstudio_embedding_adapter",
    "adapters.lmstudio_chat_adapter",
    "adapters.openalex_adapter",
    "utils.hashing",
    "cli.warmup_snapshots",
]


def test_stage_modules_are_importable() -> None:
    for module_name in MODULES:
        importlib.import_module(module_name)
