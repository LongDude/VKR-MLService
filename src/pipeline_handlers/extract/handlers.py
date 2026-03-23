from __future__ import annotations

import contextlib
import gzip
import io as io_lib
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Any, List, Optional

try:
    import pdfplumber
except ImportError:  # pragma: no cover
    pdfplumber = None  # type: ignore[assignment]

try:
    from trafilatura import extract as trafilatura_extract
except ImportError:  # pragma: no cover
    trafilatura_extract = None  # type: ignore[assignment]

from ProcessingPipeline.TextNormalisation import TextNormalisation
from pipeline_core.contracts import ArtifactRef, PipelineContext, StageResult
from pipeline_handlers.base import BaseExtractHandler


def _sanitize_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", value or "")
    return safe.strip("_") or "document"


def _looks_like_pdf(data: bytes) -> bool:
    return bool(data and data.lstrip().startswith(b"%PDF"))


def _looks_like_tex(data: bytes) -> bool:
    sample = data[:4096].decode("utf-8", errors="ignore").lower()
    return "\\documentclass" in sample or "\\begin{document}" in sample


def _safe_tar_members(members: List[tarfile.TarInfo], target_dir: Path) -> List[tarfile.TarInfo]:
    safe_members: List[tarfile.TarInfo] = []
    resolved_target = target_dir.resolve()
    for member in members:
        member_path = Path(member.name)
        if member_path.is_absolute():
            continue
        resolved = (resolved_target / member_path).resolve()
        if str(resolved).startswith(str(resolved_target)):
            safe_members.append(member)
    return safe_members


def _extract_tex_candidates(payload: bytes, target_dir: Path) -> List[Path]:
    file_obj = io_lib.BytesIO(payload)
    with contextlib.suppress(tarfile.TarError):
        with tarfile.open(fileobj=file_obj, mode="r:*") as tar:
            members = _safe_tar_members(tar.getmembers(), target_dir)
            if members:
                tar.extractall(path=target_dir, members=members)
                return sorted(path for path in target_dir.rglob("*.tex") if path.is_file())

    if payload.startswith(b"\x1f\x8b"):
        with contextlib.suppress(OSError):
            decompressed = gzip.decompress(payload)
            return _extract_tex_candidates(decompressed, target_dir)

    if _looks_like_tex(payload):
        tex_file = target_dir / "document.tex"
        tex_file.write_bytes(payload)
        return [tex_file]

    return []


def _choose_primary_tex(tex_files: List[Path]) -> Optional[Path]:
    if not tex_files:
        return None
    for tex in tex_files:
        with contextlib.suppress(OSError):
            head = tex.read_text(encoding="utf-8", errors="ignore")[:8192].lower()
            if "\\documentclass" in head or "\\begin{document}" in head:
                return tex
    return max(tex_files, key=lambda file: file.stat().st_size if file.exists() else 0)


class DatasetTextPassthroughHandler(BaseExtractHandler):
    handler_id = "dataset_text_passthrough"
    priority = 10

    def can_handle(self, ctx: PipelineContext) -> bool:
        if (ctx.source_type or "").lower() != "cold_dataset":
            return False
        return bool(ctx.runtime_data.get("clean_text") or ctx.metadata.get("clean_text"))

    def recoverable(self, err: Exception) -> bool:
        return True

    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        text = ctx.runtime_data.get("clean_text") or ctx.metadata.get("clean_text")
        if not text:
            return StageResult.skip(ctx)
        normalized = TextNormalisation().normalize(str(text)).strip()
        if not normalized:
            return StageResult.recoverable_error(
                ctx,
                code="dataset_text_empty",
                message="Dataset clean text is empty after normalization",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )
        ctx.runtime_data["clean_text"] = normalized
        ctx.register_artifact(
            ArtifactRef(
                name="clean_text",
                path=ctx.source_ref,
                source="cold_dataset",
                content_type="text/plain",
            )
        )
        return StageResult.success(ctx)


class TexPandocHandler(BaseExtractHandler):
    handler_id = "tex_pandoc"
    priority = 20

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config=config)
        self.text_normalizer = TextNormalisation()
        self.pandoc_timeout_seconds = max(30, int(self.config.get("pandoc_timeout_seconds", 120)))
        self.html_bucket = str(self.config.get("html_bucket", "converted-html-v2"))
        self.text_bucket = str(self.config.get("text_bucket", "processed-text-v2"))

    def can_handle(self, ctx: PipelineContext) -> bool:
        raw_bytes = ctx.runtime_data.get("raw_bytes")
        return isinstance(raw_bytes, (bytes, bytearray)) and not _looks_like_pdf(bytes(raw_bytes))

    def recoverable(self, err: Exception) -> bool:
        return True

    def _convert_tex_to_html(self, tex_path: Path) -> str:
        pandoc_bin = shutil.which("pandoc")
        if not pandoc_bin:
            raise RuntimeError("pandoc_not_found")
        cmd = [pandoc_bin, tex_path.name, "--from=latex", "--to=html", "--standalone"]
        process = subprocess.run(
            cmd,
            cwd=str(tex_path.parent),
            capture_output=True,
            text=True,
            check=False,
            timeout=self.pandoc_timeout_seconds,
        )
        if process.returncode != 0 or not process.stdout:
            raise RuntimeError("pandoc_failed")
        return process.stdout

    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        raw_bytes = bytes(ctx.runtime_data.get("raw_bytes") or b"")
        if not raw_bytes:
            return StageResult.recoverable_error(
                ctx,
                code="raw_bytes_missing",
                message="Raw bytes are missing for tex extraction",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )
        doc_id = _sanitize_name(ctx.article_id or ctx.source_ref or "article")
        cache_html_key = f"{doc_id}/rendered.html"
        cache_text_key = f"{doc_id}/clean.txt"
        cached_html = io.cache.read_bytes("html", cache_html_key)
        cached_text = io.cache.read_bytes("clean_text", cache_text_key)
        if cached_text is not None:
            clean_text = cached_text.decode("utf-8", errors="ignore")
            ctx.runtime_data["clean_text"] = clean_text
            if cached_html is not None:
                ctx.runtime_data["html_content"] = cached_html.decode("utf-8", errors="ignore")
            return StageResult.success(ctx)

        with tempfile.TemporaryDirectory(prefix="pipeline-tex-") as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            tex_files = _extract_tex_candidates(raw_bytes, temp_dir)
            primary = _choose_primary_tex(tex_files)
            if not primary:
                return StageResult.recoverable_error(
                    ctx,
                    code="tex_not_found",
                    message="No TEX entrypoint found in source payload",
                    stage=self.stage.value,
                    handler_id=self.handler_id,
                )
            try:
                html = self._convert_tex_to_html(primary)
            except subprocess.TimeoutExpired:
                return StageResult.recoverable_error(
                    ctx,
                    code="pandoc_timeout",
                    message="Pandoc timed out",
                    stage=self.stage.value,
                    handler_id=self.handler_id,
                )
            except RuntimeError as exc:
                return StageResult.recoverable_error(
                    ctx,
                    code=str(exc),
                    message=str(exc),
                    stage=self.stage.value,
                    handler_id=self.handler_id,
                )

        if trafilatura_extract is None:
            return StageResult.recoverable_error(
                ctx,
                code="trafilatura_missing",
                message="trafilatura is not installed",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )
        text = trafilatura_extract(html, output_format="txt", no_fallback=True) or ""
        if not text:
            text = trafilatura_extract(html, output_format="txt") or ""
        clean_text = self.text_normalizer.normalize(text).strip()
        if not clean_text:
            return StageResult.recoverable_error(
                ctx,
                code="text_empty",
                message="Text extracted from TEX is empty",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )

        io.hot_store.ensure_bucket(self.html_bucket)
        io.hot_store.ensure_bucket(self.text_bucket)
        html_object = f"{doc_id}.html"
        text_object = f"{doc_id}.txt"
        io.hot_store.put_bytes(self.html_bucket, html_object, html.encode("utf-8"), "text/html")
        io.hot_store.put_bytes(self.text_bucket, text_object, clean_text.encode("utf-8"), "text/plain")

        io.cache.write_bytes("html", cache_html_key, html.encode("utf-8"), "text/html")
        io.cache.write_bytes("clean_text", cache_text_key, clean_text.encode("utf-8"), "text/plain")

        ctx.runtime_data["html_content"] = html
        ctx.runtime_data["clean_text"] = clean_text
        ctx.register_artifact(
            ArtifactRef(
                name="html",
                bucket=self.html_bucket,
                object_name=html_object,
                source="hot_object_store",
                content_type="text/html",
            )
        )
        ctx.register_artifact(
            ArtifactRef(
                name="clean_text",
                bucket=self.text_bucket,
                object_name=text_object,
                source="hot_object_store",
                content_type="text/plain",
            )
        )
        return StageResult.success(ctx)


class PdfExtractFallbackHandler(BaseExtractHandler):
    handler_id = "pdf_extract_fallback"
    priority = 30

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config=config)
        self.text_normalizer = TextNormalisation()
        self.text_bucket = str(self.config.get("text_bucket", "processed-text-v2"))

    def can_handle(self, ctx: PipelineContext) -> bool:
        raw_bytes = ctx.runtime_data.get("raw_bytes")
        return isinstance(raw_bytes, (bytes, bytearray))

    def recoverable(self, err: Exception) -> bool:
        return False

    def handle(self, ctx: PipelineContext, io: Any) -> StageResult:
        raw_bytes = bytes(ctx.runtime_data.get("raw_bytes") or b"")
        if not raw_bytes:
            return StageResult.fatal_error(
                ctx,
                code="raw_bytes_missing",
                message="Raw bytes are missing for PDF fallback",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )

        doc_id = _sanitize_name(ctx.article_id or ctx.source_ref or "article")
        cache_text_key = f"{doc_id}/clean.txt"
        cached_text = io.cache.read_bytes("clean_text", cache_text_key)
        if cached_text is not None:
            clean_text = cached_text.decode("utf-8", errors="ignore")
            ctx.runtime_data["clean_text"] = clean_text
            return StageResult.success(ctx)

        if not _looks_like_pdf(raw_bytes):
            return StageResult.fatal_error(
                ctx,
                code="pdf_fallback_input_invalid",
                message="Payload is not PDF and TEX extraction failed",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )

        extracted_parts: List[str] = []
        if pdfplumber is None:
            return StageResult.fatal_error(
                ctx,
                code="pdfplumber_missing",
                message="pdfplumber is not installed",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )
        with pdfplumber.open(io_lib.BytesIO(raw_bytes)) as pdf:
            for page in pdf.pages:
                extracted_parts.append(page.extract_text() or "")
        raw_text = "\n".join(part for part in extracted_parts if part)
        clean_text = self.text_normalizer.normalize(raw_text).strip()
        if not clean_text:
            return StageResult.fatal_error(
                ctx,
                code="pdf_text_empty",
                message="PDF fallback produced empty text",
                stage=self.stage.value,
                handler_id=self.handler_id,
            )

        io.hot_store.ensure_bucket(self.text_bucket)
        text_object = f"{doc_id}.txt"
        io.hot_store.put_bytes(self.text_bucket, text_object, clean_text.encode("utf-8"), "text/plain")
        io.cache.write_bytes("clean_text", cache_text_key, clean_text.encode("utf-8"), "text/plain")

        ctx.runtime_data["clean_text"] = clean_text
        ctx.register_artifact(
            ArtifactRef(
                name="clean_text",
                bucket=self.text_bucket,
                object_name=text_object,
                source="hot_object_store",
                content_type="text/plain",
            )
        )
        return StageResult.success(ctx)


def get_handlers() -> List[type[BaseExtractHandler]]:
    return [DatasetTextPassthroughHandler, TexPandocHandler, PdfExtractFallbackHandler]
