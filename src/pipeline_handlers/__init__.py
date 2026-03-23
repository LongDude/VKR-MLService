from __future__ import annotations

def load_builtin_handlers(registry) -> None:
    from pipeline_handlers.acquire import get_handlers as get_acquire_handlers
    from pipeline_handlers.extract import get_handlers as get_extract_handlers
    from pipeline_handlers.ingest import get_handlers as get_ingest_handlers
    from pipeline_handlers.keywords import get_handlers as get_keyword_handlers
    from pipeline_handlers.publish import get_handlers as get_publish_handlers

    registry.register_many(get_ingest_handlers())
    registry.register_many(get_acquire_handlers())
    registry.register_many(get_extract_handlers())
    registry.register_many(get_keyword_handlers())
    registry.register_many(get_publish_handlers())


__all__ = ["load_builtin_handlers"]
