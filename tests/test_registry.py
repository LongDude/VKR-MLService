import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline_core.contracts import PipelineContext, StageName, StageResult
from pipeline_core.registry import HandlerRegistry
from pipeline_handlers.base import BaseIngestHandler


class _HandlerA(BaseIngestHandler):
    handler_id = "handler_a"
    stage = StageName.INGEST
    priority = 20

    def can_handle(self, ctx: PipelineContext) -> bool:
        return True

    def recoverable(self, err: Exception) -> bool:
        return True

    def handle(self, ctx: PipelineContext, io):
        return StageResult.success(ctx)


class _HandlerB(BaseIngestHandler):
    handler_id = "handler_b"
    stage = StageName.INGEST
    priority = 10

    def can_handle(self, ctx: PipelineContext) -> bool:
        return True

    def recoverable(self, err: Exception) -> bool:
        return True

    def handle(self, ctx: PipelineContext, io):
        return StageResult.success(ctx)


class HandlerRegistryTest(unittest.TestCase):
    def test_resolve_chain_by_priority(self):
        registry = HandlerRegistry()
        registry.register_many([_HandlerA, _HandlerB])
        chain = registry.resolve_chain(StageName.INGEST)
        self.assertEqual([handler.handler_id for handler in chain], ["handler_b", "handler_a"])

    def test_resolve_chain_by_enabled_list(self):
        registry = HandlerRegistry()
        registry.register_many([_HandlerA, _HandlerB])
        chain = registry.resolve_chain(StageName.INGEST, enabled_handlers=["handler_a"])
        self.assertEqual([handler.handler_id for handler in chain], ["handler_a"])

    def test_empty_enabled_list_disables_stage(self):
        registry = HandlerRegistry()
        registry.register_many([_HandlerA, _HandlerB])
        chain = registry.resolve_chain(StageName.INGEST, enabled_handlers=[])
        self.assertEqual(chain, [])


if __name__ == "__main__":
    unittest.main()
