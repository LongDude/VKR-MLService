from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR.parent
PROJECT_DIR = SRC_DIR.parent

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from adapters import QdrantAdapter
from core.exceptions import AppError
from ml.services.qdrant_collections import QdrantCollectionInitializer

CollectionName = str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for Qdrant collection initialization."""
    parser = argparse.ArgumentParser(
        description="Initialize ML-service Qdrant collections and payload indexes.",
    )
    parser.add_argument(
        "vector_size",
        type=int,
        help="Embedding vector size for Qdrant collections.",
    )
    parser.add_argument(
        "--collection",
        choices=[
            "all",
            "papers",
            "research_entities",
            "trend_clusters",
            "trend_cluster_periods",
            "user_profiles",
        ],
        default="all",
        help="Collection group to initialize. Defaults to all.",
    )
    parser.add_argument(
        "--distance",
        choices=["Cosine", "Dot", "Euclid", "Manhattan"],
        default=os.getenv("QDRANT_DISTANCE", "Cosine"),
        help="Qdrant vector distance. Defaults to QDRANT_DISTANCE or Cosine.",
    )
    parser.add_argument(
        "--env-file",
        default=str(PROJECT_DIR / ".env"),
        help="Path to .env file. Defaults to project .env.",
    )
    parser.add_argument(
        "--qdrant-url",
        default=None,
        help="Qdrant URL. Defaults to QDRANT_URL when set.",
    )
    parser.add_argument(
        "--qdrant-host",
        default=None,
        help="Qdrant host. Defaults to QDRANT_HOST or localhost.",
    )
    parser.add_argument(
        "--qdrant-port",
        type=int,
        default=None,
        help="Qdrant port. Defaults to QDRANT_PORT or 6333.",
    )
    parser.add_argument(
        "--qdrant-api-key",
        default=None,
        help="Qdrant API key. Defaults to QDRANT_API_KEY when set.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Initialize requested Qdrant collections and print a JSON status."""
    args = parse_args(argv)
    load_dotenv(args.env_file)

    try:
        initializer = QdrantCollectionInitializer(
            build_qdrant_adapter(args),
            distance=args.distance,
        )
        ensure_collection(initializer, args.collection, args.vector_size)
    except AppError as exc:
        print_json(exc.to_dict(), stream=sys.stderr)
        return 1
    except Exception as exc:
        print_json(
            {
                "error": {
                    "code": exc.__class__.__name__,
                    "message": str(exc),
                    "details": {},
                }
            },
            stream=sys.stderr,
        )
        return 1

    print_json(
        {
            "status": "ok",
            "collection": args.collection,
            "vector_size": args.vector_size,
            "distance": args.distance,
        }
    )
    return 0


def ensure_collection(
    initializer: QdrantCollectionInitializer,
    collection: CollectionName,
    vector_size: int,
) -> None:
    """Dispatch collection initialization to QdrantCollectionInitializer."""
    handlers: dict[CollectionName, Callable[[int], None]] = {
        "all": initializer.ensure_all,
        "papers": initializer.ensure_papers_collection,
        "research_entities": initializer.ensure_research_entities_collection,
        "trend_clusters": initializer.ensure_trend_clusters_collection,
        "trend_cluster_periods": initializer.ensure_trend_cluster_periods_collection,
        "user_profiles": initializer.ensure_user_profiles_collection,
    }
    handlers[collection](vector_size)


def build_qdrant_adapter(args: argparse.Namespace) -> QdrantAdapter:
    """Build QdrantAdapter from CLI arguments or environment variables."""
    qdrant_url = args.qdrant_url or os.getenv("QDRANT_URL")
    api_key = args.qdrant_api_key or os.getenv("QDRANT_API_KEY")
    if qdrant_url:
        return QdrantAdapter(url=qdrant_url, api_key=api_key)

    return QdrantAdapter(
        host=args.qdrant_host or os.getenv("QDRANT_HOST") or "localhost",
        port=args.qdrant_port or _optional_int_env("QDRANT_PORT") or 6333,
        api_key=api_key,
    )


def _optional_int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    return int(value)


def print_json(payload: dict[str, Any], *, stream: Any = sys.stdout) -> None:
    """Print JSON with stable UTF-8 output."""
    print(json.dumps(payload, ensure_ascii=False, indent=2), file=stream)


if __name__ == "__main__":
    raise SystemExit(main())
