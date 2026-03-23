import tempfile
import textwrap
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline_core.contracts import PipelineContext, StageName, StageResult
from pipeline_core.manager import PipelineManager
from pipeline_core.registry import HandlerRegistry
from pipeline_handlers.base import (
    BaseAcquireHandler,
    BaseExtractHandler,
    BaseIngestHandler,
    BaseKeywordHandler,
    BasePublishHandler,
)


class _MemoryCache:
    def __init__(self) -> None:
        self._data = {}

    def read_bytes(self, boundary, key):
        return self._data.get((boundary, key))

    def write_bytes(self, boundary, key, data, content_type):
        self._data[(boundary, key)] = data
        return f"{boundary}/{key}"


class _FakeIO:
    def __init__(self):
        self._records = [
            (
                "1-0",
                {
                    "source_type": "hot_arxiv",
                    "source_ref": "http://example.com/mock.pdf",
                    "article_id": "a1",
                    "metadata": {"id": "a1"},
                },
            )
        ]
        self.acked = []
        self.keywords_payloads = []
        self.errors_payloads = []
        self.cache = _MemoryCache()

    def poll_ingress(self):
        if not self._records:
            return None
        return self._records.pop(0)

    def ack_ingress(self, stream_id):
        self.acked.append(stream_id)

    def publish_keywords(self, payload):
        self.keywords_payloads.append(payload)

    def publish_error(self, payload):
        self.errors_payloads.append(payload)


class _IngestOK(BaseIngestHandler):
    handler_id = "test_ingest_ok"
    stage = StageName.INGEST
    priority = 10

    def can_handle(self, ctx: PipelineContext) -> bool:
        return True

    def recoverable(self, err: Exception) -> bool:
        return False

    def handle(self, ctx: PipelineContext, io):
        ctx.metadata["ingested"] = True
        return StageResult.success(ctx)


class _AcquireOK(BaseAcquireHandler):
    handler_id = "test_acquire_ok"
    stage = StageName.ACQUIRE
    priority = 10

    def can_handle(self, ctx: PipelineContext) -> bool:
        return True

    def recoverable(self, err: Exception) -> bool:
        return False

    def handle(self, ctx: PipelineContext, io):
        ctx.runtime_data["raw_bytes"] = b"%PDF-1.4 test"
        return StageResult.success(ctx)


class _ExtractRecoverable(BaseExtractHandler):
    handler_id = "test_extract_recoverable"
    stage = StageName.EXTRACT
    priority = 10

    def can_handle(self, ctx: PipelineContext) -> bool:
        return True

    def recoverable(self, err: Exception) -> bool:
        return True

    def handle(self, ctx: PipelineContext, io):
        return StageResult.recoverable_error(
            ctx,
            code="mock_recoverable",
            message="recoverable fail",
            stage=self.stage.value,
            handler_id=self.handler_id,
        )


class _ExtractSuccess(BaseExtractHandler):
    handler_id = "test_extract_success"
    stage = StageName.EXTRACT
    priority = 20

    def can_handle(self, ctx: PipelineContext) -> bool:
        return True

    def recoverable(self, err: Exception) -> bool:
        return False

    def handle(self, ctx: PipelineContext, io):
        ctx.runtime_data["clean_text"] = "sample clean text"
        return StageResult.success(ctx)


class _KeywordsOK(BaseKeywordHandler):
    handler_id = "test_keywords_ok"
    stage = StageName.KEYWORDS
    priority = 10

    def can_handle(self, ctx: PipelineContext) -> bool:
        return True

    def recoverable(self, err: Exception) -> bool:
        return False

    def handle(self, ctx: PipelineContext, io):
        ctx.runtime_data["keywords"] = [{"canonical": "pipeline", "relativity": 0.9}]
        return StageResult.success(ctx)


class _PublishOK(BasePublishHandler):
    handler_id = "test_publish_ok"
    stage = StageName.PUBLISH
    priority = 10

    def can_handle(self, ctx: PipelineContext) -> bool:
        return True

    def recoverable(self, err: Exception) -> bool:
        return False

    def handle(self, ctx: PipelineContext, io):
        io.publish_keywords({"article_id": ctx.article_id, "keywords": ctx.runtime_data["keywords"]})
        return StageResult.success(ctx)


class PipelineManagerFallbackTest(unittest.TestCase):
    def test_fallback_switches_after_recoverable(self):
        registry = HandlerRegistry()
        registry.register_many(
            [
                _IngestOK,
                _AcquireOK,
                _ExtractRecoverable,
                _ExtractSuccess,
                _KeywordsOK,
                _PublishOK,
            ]
        )
        fake_io = _FakeIO()

        with tempfile.TemporaryDirectory(prefix="pipeline-test-") as temp_dir_name:
            config_path = Path(temp_dir_name) / "pipeline.yaml"
            config_path.write_text(
                textwrap.dedent(
                    """
                    default_profile: test
                    io:
                      buffer_size_bytes: 4096
                    profiles:
                      test:
                        streams:
                          ingress: pipeline_ingress_v2
                          keywords: pipeline_keywords_v2
                          errors: pipeline_errors_v2
                        cache_bucket: pipeline-cache-v2
                        cache: {}
                        handlers:
                          ingest: [test_ingest_ok]
                          acquire: [test_acquire_ok]
                          extract: [test_extract_recoverable, test_extract_success]
                          keywords: [test_keywords_ok]
                          publish: [test_publish_ok]
                        handler_configs: {}
                    """
                ).strip(),
                encoding="utf-8",
            )

            manager = PipelineManager(
                config_path=str(config_path),
                profile="test",
                registry=registry,
                io=fake_io,
            )
            processed = manager.run_once()

        self.assertTrue(processed)
        self.assertEqual(fake_io.acked, ["1-0"])
        self.assertEqual(len(fake_io.errors_payloads), 0)
        self.assertEqual(len(fake_io.keywords_payloads), 1)
        self.assertEqual(fake_io.keywords_payloads[0]["article_id"], "a1")


if __name__ == "__main__":
    unittest.main()
