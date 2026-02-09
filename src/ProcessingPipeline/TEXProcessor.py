"""Processor that converts LaTeX archives to HTML and text outputs."""

from __future__ import annotations

import contextlib
import gzip
import io
import json
import logging
import re
import shutil
import subprocess
import tarfile
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import redis
from minio.error import S3Error

from service_lib import ServiceConnectionFactory
from ProcessingPipeline.TextNormalisation import TextNormalisation

try:
    from trafilatura import extract as trafilatura_extract
except ImportError:  # pragma: no cover
    trafilatura_extract = None  # type: ignore[assignment]


logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    tex_files: List[Path]
    handled: bool = False
    reason: Optional[str] = None


class TEXProcessor:
    """Process LaTeX source archives from Redis queue and publish derived artifacts."""

    def __init__(
        self,
        redis_preprocess_stream_key: str = "preprocess_queue",
        redis_postprocess_stream_key: str = "postprocess_queue",
        redis_error_stream_key: str = "preprocess_err_queue",
        source_bucket: str = "pdf-raw",
        converted_html_bucket: str = "converted-html",
        processed_text_bucket: str = "processed-text",
        pdf_bucket: str = "pdf-papers",
        failed_source_bucket: str = "failed-src",
        redis_stage_streams: Optional[Dict[str, str]] = None,
    ) -> None:
        self.redis_preprocess_stream_key = redis_preprocess_stream_key
        self.redis_postprocess_stream_key = redis_postprocess_stream_key
        self.redis_error_stream_key = redis_error_stream_key
        self.source_bucket = source_bucket
        self.converted_html_bucket = converted_html_bucket
        self.processed_text_bucket = processed_text_bucket
        self.pdf_bucket = pdf_bucket
        self.failed_source_bucket = failed_source_bucket
        self.redis_stage_streams = redis_stage_streams or {
            "downloaded": "tex_stage_downloaded",
            "extracted": "tex_stage_extracted",
            "html": "tex_stage_html_ready",
            "text": "tex_stage_text_ready",
            "pdf_redirect": "tex_stage_pdf_redirect",
            "failed": "tex_stage_failed",
        }
        self.redis_client = ServiceConnectionFactory.getRedisClient()
        self.minio_client = ServiceConnectionFactory.getMinioClient()
        self.text_normalizer = TextNormalisation()
        self.processed_count = 0
        self._ensured_buckets: set[str] = set()
        for bucket in {
            self.source_bucket,
            self.converted_html_bucket,
            self.processed_text_bucket,
            self.pdf_bucket,
            self.failed_source_bucket,
        }:
            if bucket:
                self._ensure_bucket(bucket)

    def _ensure_bucket(self, bucket_name: str) -> None:
        if not bucket_name or bucket_name in self._ensured_buckets:
            return
        try:
            if not self.minio_client.bucket_exists(bucket_name):
                self.minio_client.make_bucket(bucket_name)
                logger.info("Created missing Minio bucket %s", bucket_name)
            self._ensured_buckets.add(bucket_name)
        except S3Error as exc:
            logger.error("Unable to prepare Minio bucket %s: %s", bucket_name, exc)
            raise

    def _upload_bytes(self, bucket_name: str, object_name: str, data: bytes, content_type: str) -> bool:
        payload = io.BytesIO(data)
        try:
            self.minio_client.put_object(
                bucket_name,
                object_name,
                payload,
                length=len(data),
                content_type=content_type,
            )
            return True
        except S3Error as exc:
            logger.error("Unable to upload %s to bucket %s: %s", object_name, bucket_name, exc)
            return False

    def _record_stage(self, stage: str, record: Dict[str, Any], **extra: Any) -> None:
        stream_key = self.redis_stage_streams.get(stage)
        if not stream_key:
            return
        payload = {**self._clean_record(record), "stage": stage, **extra}
        self._publish_stream_payload(stream_key, payload)

    def _get_last_record(self) -> Optional[Dict[str, Any]]:
        try:
            entries = self.redis_client.xrevrange(self.redis_preprocess_stream_key, count=1)
            if not entries:
                return None
            stream_id, payload = entries[0]
            parsed = {key: json.loads(value) for key, value in payload.items()}
            record = parsed.get("data")
            if not isinstance(record, dict):
                return None
            record["stream_id"] = stream_id
            return record
        except (redis.RedisError, json.JSONDecodeError) as exc:
            logger.error("Unable to read %s: %s", self.redis_preprocess_stream_key, exc)
            return None

    def _remove_stream_record(self, stream_id: str) -> None:
        if not stream_id:
            return
        try:
            self.redis_client.xdel(self.redis_preprocess_stream_key, stream_id)
        except redis.RedisError as exc:
            logger.warning("Unable to delete record %s: %s", stream_id, exc)

    def _publish_stream_payload(self, stream_key: str, payload: Dict[str, Any]) -> None:
        try:
            self.redis_client.xadd(stream_key, {"data": json.dumps(payload, ensure_ascii=False)})
        except redis.RedisError as exc:
            logger.error("Unable to publish to %s: %s", stream_key, exc)

    def _clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return {key: value for key, value in record.items() if key != "stream_id"}

    def _move_to_error_queue(self, record: Dict[str, Any], reason: str) -> None:
        payload = self._clean_record(record)
        payload["error"] = reason
        self._record_stage("failed", record, reason=reason)
        self._publish_stream_payload(self.redis_error_stream_key, payload)
        stream_id = record.get("stream_id")
        if stream_id:
            self._remove_stream_record(stream_id)

    def _publish_processed_record(self, record: Dict[str, Any], html_path: str, text_path: str) -> None:
        payload = {
            **self._clean_record(record),
            "html_bucket": self.converted_html_bucket,
            "html_path": html_path,
            "text_bucket": self.processed_text_bucket,
            "text_path": text_path,
            "source_object": record.get("pdf_path"),
            "processed_at": datetime.utcnow().isoformat() + "Z",
        }
        self._publish_stream_payload(self.redis_postprocess_stream_key, payload)

    def _build_base_name(self, record: Dict[str, Any]) -> str:
        candidate = record.get("pdf_path") or record.get("article_id") or "document"
        candidate = candidate.split("/")[-1].strip()
        for suffix in (".tar.gz", ".tar", ".tgz", ".gz", ".tex", ".pdf"):
            if candidate.lower().endswith(suffix):
                candidate = candidate[: -len(suffix)]
                break
        candidate = re.sub(r"[^a-zA-Z0-9_.-]", "_", candidate)
        return candidate or "document"

    def _download_archive(self, object_name: str) -> Optional[bytes]:
        if not object_name:
            logger.error("Empty source path received")
            return None
        response = None
        try:
            response = self.minio_client.get_object(self.source_bucket, object_name)
            data = response.read()
            if not data:
                logger.warning("Object %s is empty", object_name)
                return None
            return data
        except S3Error as exc:
            logger.error("Unable to download %s from %s: %s", object_name, self.source_bucket, exc)
            return None
        finally:
            if response:
                with contextlib.suppress(Exception):
                    response.close()
                    response.release_conn()

    def _extract_to_temp(self, record: Dict[str, Any], archive_bytes: bytes, temp_dir: Path) -> ExtractionResult:
        if self._extract_tar_bytes(archive_bytes, temp_dir):
            tex_files = self._gather_tex_files(temp_dir)
            if tex_files:
                return ExtractionResult(tex_files=tex_files)
            pdf_files = [path for path in temp_dir.rglob("*.pdf") if path.is_file()]
            if pdf_files:
                handled = self._handle_pdf_redirect(record, pdf_files[0].read_bytes())
                return ExtractionResult(tex_files=[], handled=handled, reason=None if handled else "pdf_upload_failed")
            return ExtractionResult(tex_files=[], handled=False, reason="tex_not_found")
        if archive_bytes.startswith(b"\x1f\x8b"):
            gzip_result = self._extract_from_gzip(record, archive_bytes, temp_dir)
            if gzip_result:
                return gzip_result
        if self._looks_like_pdf(archive_bytes):
            handled = self._handle_pdf_redirect(record, archive_bytes)
            return ExtractionResult(tex_files=[], handled=handled, reason=None if handled else "pdf_upload_failed")
        if self._looks_like_tex(archive_bytes):
            tex_path = self._write_single_tex(temp_dir, archive_bytes)
            return ExtractionResult(tex_files=[tex_path])
        return ExtractionResult(tex_files=[], handled=False, reason="tex_not_found")

    def _extract_from_gzip(self, record: Dict[str, Any], archive_bytes: bytes, temp_dir: Path) -> Optional[ExtractionResult]:
        try:
            decompressed = gzip.decompress(archive_bytes)
        except OSError:
            return None
        if self._extract_tar_bytes(decompressed, temp_dir):
            tex_files = self._gather_tex_files(temp_dir)
            if tex_files:
                return ExtractionResult(tex_files=tex_files)
            pdf_files = [path for path in temp_dir.rglob("*.pdf") if path.is_file()]
            if pdf_files:
                handled = self._handle_pdf_redirect(record, pdf_files[0].read_bytes())
                return ExtractionResult(tex_files=[], handled=handled, reason=None if handled else "pdf_upload_failed")
            return ExtractionResult(tex_files=[], handled=False, reason="tex_not_found")
        if self._looks_like_pdf(decompressed):
            handled = self._handle_pdf_redirect(record, decompressed)
            return ExtractionResult(tex_files=[], handled=handled, reason=None if handled else "pdf_upload_failed")
        if self._looks_like_tex(decompressed):
            tex_path = self._write_single_tex(temp_dir, decompressed)
            return ExtractionResult(tex_files=[tex_path])
        return ExtractionResult(tex_files=[], handled=False, reason="tex_not_found")

    def _extract_tar_bytes(self, data: bytes, target_dir: Path) -> bool:
        file_obj = io.BytesIO(data)
        try:
            with tarfile.open(fileobj=file_obj, mode="r:*") as tar:
                members = self._safe_tar_members(tar.getmembers(), target_dir)
                if not members:
                    return False
                tar.extractall(path=target_dir, members=members)
            return True
        except tarfile.TarError as exc:
            logger.debug("Payload is not a tar archive: %s", exc)
            return False

    def _safe_tar_members(self, members: Iterable[tarfile.TarInfo], target_dir: Path) -> List[tarfile.TarInfo]:
        safe_members: List[tarfile.TarInfo] = []
        resolved_target = target_dir.resolve()
        for member in members:
            member_path = Path(member.name)
            if member_path.is_absolute():
                continue
            try:
                resolved = (resolved_target / member_path).resolve()
            except OSError:
                continue
            if not str(resolved).startswith(str(resolved_target)):
                continue
            safe_members.append(member)
        return safe_members

    def _gather_tex_files(self, temp_dir: Path) -> List[Path]:
        return sorted([path for path in temp_dir.rglob("*.tex") if path.is_file()])

    def _write_single_tex(self, temp_dir: Path, data: bytes) -> Path:
        target = temp_dir / f"document-{uuid.uuid4().hex}.tex"
        target.write_bytes(data)
        return target

    def _looks_like_pdf(self, data: bytes) -> bool:
        return data.lstrip().startswith(b"%PDF")

    def _looks_like_tex(self, data: bytes) -> bool:
        sample = data[:4096].decode("utf-8", errors="ignore").lower()
        return "\\documentclass" in sample or "\\begin{document}" in sample

    def _select_primary_tex_file(self, tex_files: List[Path]) -> Optional[Path]:
        if not tex_files:
            return None
        for tex_path in sorted(tex_files):
            try:
                head = tex_path.open("r", encoding="utf-8", errors="ignore").read(8192)
            except OSError:
                continue
            lowered = head.lower()
            if "\\documentclass" in lowered or "\\begin{document}" in lowered:
                return tex_path
        sized: List[tuple[int, Path]] = []
        for tex_path in tex_files:
            try:
                sized.append((tex_path.stat().st_size, tex_path))
            except OSError:
                sized.append((0, tex_path))
        if not sized:
            return None
        sized.sort(key=lambda item: item[0])
        return sized[-1][1]

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
        )
        if process.returncode != 0:
            logger.error("pandoc failed for %s: %s", tex_path, process.stderr.strip())
            raise RuntimeError("pandoc_failed")
        if not process.stdout:
            raise RuntimeError("pandoc_failed")
        return process.stdout

    def _strip_functions_from_html(self, html: str) -> str:
        if not html:
            return ""
        script_pattern = re.compile(r"<script\\b[^>]*>.*?</script>", re.IGNORECASE | re.DOTALL)
        function_pattern = re.compile(r"function\\s+[a-zA-Z0-9_]*\\s*\\([^)]*\\)\\s*\\{.*?\\}", re.DOTALL)
        without_scripts = script_pattern.sub("", html)
        return function_pattern.sub("", without_scripts)

    def _extract_plain_text(self, html: str) -> str:
        if not trafilatura_extract:
            raise RuntimeError("trafilatura_missing")
        text = trafilatura_extract(html, output_format="txt", no_fallback=True) or ""
        if not text:
            text = trafilatura_extract(html, output_format="txt") or ""
        text = self.text_normalizer.normalize(text)
        return text.strip()

    def _save_failed_source(self, record: Dict[str, Any], archive_bytes: bytes, reason: str) -> None:
        if not self.failed_source_bucket or not archive_bytes:
            return
        base_name = self._build_base_name(record)
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        object_name = f"{base_name}-{reason}-{timestamp}.bin"
        self._upload_bytes(self.failed_source_bucket, object_name, archive_bytes, "application/octet-stream")

    def _handle_pdf_redirect(self, record: Dict[str, Any], pdf_bytes: bytes) -> bool:
        if not pdf_bytes:
            return False
        object_name = f"{self._build_base_name(record)}.pdf"
        success = self._upload_bytes(self.pdf_bucket, object_name, pdf_bytes, "application/pdf")
        if success:
            self._record_stage("pdf_redirect", record, pdf_path=object_name, bucket=self.pdf_bucket)
        return success

    def process_last_record(self) -> bool:
        record = self._get_last_record()
        if not record:
            return False
        object_name = record.get("pdf_path")
        archive_bytes = self._download_archive(object_name)
        if not archive_bytes:
            self._move_to_error_queue(record, "archive_missing")
            return False
        self._record_stage("downloaded", record, object_name=object_name)
        with tempfile.TemporaryDirectory(prefix="texproc-") as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            extraction = self._extract_to_temp(record, archive_bytes, temp_dir)
            if extraction.handled:
                stream_id = record.get("stream_id")
                if stream_id:
                    self._remove_stream_record(stream_id)
                logger.info("Record %s redirected to PDF bucket", record.get("article_id") or object_name)
                return True
            if extraction.reason and not extraction.tex_files:
                self._save_failed_source(record, archive_bytes, extraction.reason)
                self._move_to_error_queue(record, extraction.reason)
                return False
            tex_files = extraction.tex_files
            primary_tex = self._select_primary_tex_file(tex_files)
            if not primary_tex:
                self._save_failed_source(record, archive_bytes, "primary_tex_missing")
                self._move_to_error_queue(record, "primary_tex_missing")
                return False
            try:
                relative_primary = str(primary_tex.relative_to(temp_dir))
            except ValueError:
                relative_primary = str(primary_tex)
            self._record_stage("extracted", record, tex_count=len(tex_files), primary_tex=relative_primary)
            try:
                html_content = self._convert_tex_to_html(primary_tex)
            except RuntimeError as exc:
                reason = str(exc)
                self._save_failed_source(record, archive_bytes, reason)
                self._move_to_error_queue(record, reason)
                return False
            base_name = self._build_base_name(record)
            html_object_name = f"{base_name}.html"
            if not self._upload_bytes(
                self.converted_html_bucket,
                html_object_name,
                html_content.encode("utf-8"),
                "text/html",
            ):
                self._move_to_error_queue(record, "html_upload_failed")
                return False
            self._record_stage("html", record, html_path=html_object_name, bucket=self.converted_html_bucket)
            cleaned_html = self._strip_functions_from_html(html_content)
            logger.info("Cleaned html; processing plain text")
            try:
                plain_text = self._extract_plain_text(cleaned_html)
            except RuntimeError as exc:
                reason = str(exc)
                self._save_failed_source(record, archive_bytes, reason)
                self._move_to_error_queue(record, reason)
                return False
            if not plain_text:
                self._move_to_error_queue(record, "text_empty")
                return False
            text_object_name = f"{base_name}.txt"
            if not self._upload_bytes(
                self.processed_text_bucket,
                text_object_name,
                plain_text.encode("utf-8"),
                "text/plain",
            ):
                self._move_to_error_queue(record, "text_upload_failed")
                return False
            self._record_stage("text", record, text_path=text_object_name, bucket=self.processed_text_bucket)
            stream_id = record.get("stream_id")
            if stream_id:
                self._remove_stream_record(stream_id)
            self._publish_processed_record(record, html_object_name, text_object_name)
            self.processed_count += 1
            logger.info(
                "Processed LaTeX archive %s into HTML/text (chars=%d)",
                record.get("article_id") or object_name,
                len(plain_text),
            )
            return True

    def run(self, continuous: bool = False) -> None:
        logger.info("TEXProcessor started")
        if continuous:
            while self.process_last_record():
                continue
        else:
            self.process_last_record()
        logger.info("TEXProcessor stopped after %d articles", self.processed_count)


__all__ = ["TEXProcessor"]
if __name__ == "__main__":
    TEXProcessor().run()
