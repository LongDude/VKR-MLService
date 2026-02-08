"""Processor that extracts text from LaTeX source archives stored in Minio."""

from __future__ import annotations

import io
import json
import logging
import re
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import redis
from minio.error import S3Error

from service_lib import ServiceConnectionFactory
from ProcessingPipeline.TextNormalisation import TextNormalisation

try:
    from pylatexenc.latex2text import LatexNodes2Text
except ImportError:  # pragma: no cover
    LatexNodes2Text = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class TEXProcessor:
    """Process LaTeX source archives from Redis preprocess queue and store parsed text."""

    def __init__(
        self,
        redis_preprocess_stream_key: str = "preprocess_queue",
        redis_postprocess_stream_key: str = "postprocess_queue",
        redis_error_stream_key: str = "preprocess_err_queue",
        source_bucket: str = "pdf-raw",
        parsed_text_bucket: str = "parsed-text",
    ) -> None:
        self.redis_preprocess_stream_key = redis_preprocess_stream_key
        self.redis_postprocess_stream_key = redis_postprocess_stream_key
        self.redis_error_stream_key = redis_error_stream_key
        self.source_bucket = source_bucket
        self.parsed_text_bucket = parsed_text_bucket
        self.redis_client = ServiceConnectionFactory.getRedisClient()
        self.minio_client = ServiceConnectionFactory.getMinioClient()
        self.text_normalizer = TextNormalisation()
        self.converter = LatexNodes2Text() if LatexNodes2Text else None
        self.processed_count = 0
        self._ensure_bucket(self.source_bucket)
        self._ensure_bucket(self.parsed_text_bucket)

    def _ensure_bucket(self, bucket_name: str) -> None:
        try:
            if not self.minio_client.bucket_exists(bucket_name):
                self.minio_client.make_bucket(bucket_name)
                logger.info("Создан отсутствующий бакет Minio: %s", bucket_name)
        except S3Error as exc:
            logger.error("Не удалось подготовить бакет %s: %s", bucket_name, exc)
            raise

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
            logger.error("Не удалось прочитать %s: %s", self.redis_preprocess_stream_key, exc)
            return None

    def _remove_stream_record(self, stream_id: str) -> None:
        if not stream_id:
            return
        try:
            self.redis_client.xdel(self.redis_preprocess_stream_key, stream_id)
        except redis.RedisError as exc:
            logger.warning("Не удалось удалить запись %s: %s", stream_id, exc)

    def _publish_stream_payload(self, stream_key: str, payload: Dict[str, Any]) -> None:
        try:
            self.redis_client.xadd(stream_key, {"data": json.dumps(payload, ensure_ascii=False)})
        except redis.RedisError as exc:
            logger.error("Не удалось записать в %s: %s", stream_key, exc)

    def _clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return {key: value for key, value in record.items() if key != "stream_id"}

    def _move_to_error_queue(self, record: Dict[str, Any], reason: str) -> None:
        payload = self._clean_record(record)
        payload["error"] = reason
        self._publish_stream_payload(self.redis_error_stream_key, payload)
        stream_id = record.get("stream_id")
        if stream_id:
            self._remove_stream_record(stream_id)

    def _publish_postprocess_record(self, record: Dict[str, Any], parsed_path: str) -> None:
        payload = {
            **self._clean_record(record),
            "text_path": parsed_path,
            "original_pdf_path": record.get("pdf_path"),
        }
        self._publish_stream_payload(self.redis_postprocess_stream_key, payload)

    def _build_parsed_filename(self, record: Dict[str, Any]) -> str:
        candidate = record.get("pdf_path") or record.get("article_id") or "document"
        candidate = candidate.strip().lstrip("/")
        if ":" in candidate and not candidate.lower().startswith(("http://", "https://", "minio://")):
            candidate = candidate.split(":", 1)[-1]
        if candidate.lower().endswith(".tar.gz"):
            candidate = candidate[: -len(".tar.gz")]
        elif candidate.lower().endswith(".gz"):
            candidate = candidate[: -len(".gz")]
        elif candidate.lower().endswith(".tex"):
            candidate = candidate[: -len(".tex")]
        sanitized = re.sub(r"[^a-zA-Z0-9_.-]", "_", candidate) or "parsed_article"
        return f"{sanitized}.json"

    def _save_parsed_text(self, object_name: str, payload: Dict[str, Any]) -> bool:
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        try:
            self.minio_client.put_object(
                self.parsed_text_bucket,
                object_name,
                io.BytesIO(data),
                length=len(data),
                content_type="application/json",
            )
            return True
        except S3Error as exc:
            logger.error("Не удалось загрузить JSON %s: %s", object_name, exc)
            return False

    def _download_archive(self, object_name: str) -> Optional[bytes]:
        if not object_name:
            logger.error("Пустой путь до архива")
            return None

        response = None
        try:
            response = self.minio_client.get_object(self.source_bucket, object_name)
            data = response.read()
            if not data:
                logger.warning("Архив %s пуст", object_name)
                return None
            return data
        except S3Error as exc:
            logger.error("Не удалось скачать %s из бакета %s: %s", object_name, self.source_bucket, exc)
            return None
        finally:
            if response:
                try:
                    response.close()
                    response.release_conn()
                except Exception:
                    logger.debug("Не удалось освободить соединение Minio", exc_info=False)

    def _latex_to_text(self, source: str) -> str:
        if not source:
            return ""
        source = self._remove_footnotes(source)
        if self.converter:
            try:
                return self.converter.latex_to_text(source)
            except Exception:
                logger.debug("pylatexenc не смог обработать документ", exc_info=True)
        stripped = re.sub(r"\\begin\{.*?\}|\\end\{.*?\}", " ", source, flags=re.DOTALL)
        stripped = re.sub(r"\\[a-zA-Z@]+(\*?)({[^}]*}|\\[[^\\]]*\\])?", " ", stripped)
        stripped = re.sub(r"\$[^$]*\$", " ", stripped)
        stripped = re.sub(r"[%].*", " ", stripped)
        return stripped

    def _remove_footnotes(self, text: str) -> str:
        if not text:
            return ""
        pattern = re.compile(r"\\footnote\s*\{([^{}]*|\{[^{}]*\})*\}", re.DOTALL)
        return pattern.sub(" ", text)

    def _extract_abstract_segments(self, latex_source: str) -> List[str]:
        if not latex_source:
            return []
        segments: List[str] = []
        env_pattern = re.compile(r"\\begin\{abstract\}(.*?)\\end\{abstract\}", re.DOTALL | re.IGNORECASE)
        cmd_pattern = re.compile(r"\\abstract\{(.*?)\}", re.DOTALL | re.IGNORECASE)
        segments.extend(env_pattern.findall(latex_source))
        segments.extend(cmd_pattern.findall(latex_source))
        return segments

    def _split_references_block(self, text: str) -> Tuple[str, str]:
        markers = ["references", "bibliography", "литература", "библиография", "список литературы"]
        lower_text = text.lower()
        start = None
        for marker in markers:
            idx = lower_text.find(marker)
            if idx != -1:
                start = idx
                break
        if start is None:
            return text, ""
        return text[:start].rstrip(), text[start:].strip()

    def _extract_text_from_archive(self, archive_bytes: bytes) -> Tuple[str, List[str]]:
        texts: List[str] = []
        abstract_chunks: List[str] = []
        try:
            with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:gz") as tar:
                members = [m for m in tar.getmembers() if m.isfile() and m.name.lower().endswith(".tex")]
                if not members:
                    logger.warning("В архиве не найдено .tex файлов")
                for member in members:
                    extracted = tar.extractfile(member)
                    if not extracted:
                        continue
                    try:
                        raw = extracted.read().decode("utf-8", errors="ignore")
                    finally:
                        extracted.close()
                    for abstract_segment in self._extract_abstract_segments(raw):
                        abstract_text = self.text_normalizer.normalize(self._latex_to_text(abstract_segment))
                        if abstract_text:
                            abstract_chunks.append(abstract_text)
                    plain = self._latex_to_text(raw)
                    normalized = self.text_normalizer.normalize(plain)
                    if normalized:
                        texts.append(normalized)
        except tarfile.TarError as exc:
            logger.error("Не удалось открыть tar.gz: %s", exc)
            return "", []
        combined_text = "\n\n".join(texts).strip()
        return combined_text, abstract_chunks

    def _extract_citations(self, text: str) -> List[str]:
        markers = ["references", "bibliography", "литература", "библиография", "список литературы"]
        lower_text = text.lower()
        start = None
        for marker in markers:
            idx = lower_text.find(marker)
            if idx != -1:
                start = idx
                break
        block = text[start:] if start is not None else text[-4000:]
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        citations: List[str] = []
        number_pattern = re.compile(r"^(\[\d+\]|[0-9]+[\.\)]|-)")
        for line in lines:
            if number_pattern.match(line):
                citations.append(line)
            elif citations:
                citations[-1] += f" {line}"
        return citations

    def process_last_record(self) -> bool:
        record = self._get_last_record()
        if not record:
            return False

        download_format = (record.get("download_format") or "").lower()
        article_id = record.get("article_id") or record.get("id") or record.get("pdf_path")
        logger.info(
            "TEXProcessor получил запись: article_id=%s, format=%s, path=%s",
            article_id,
            download_format or "unknown",
            record.get("pdf_path"),
        )
        if download_format and download_format != "source":
            logger.debug("Запись %s предназначена не для TEXProcessor", record.get("article_id"))
            return False

        stream_id = record.get("stream_id")
        object_name = record.get("pdf_path")  # историческое название поля
        lower_name = (object_name or "").lower()
        if download_format != "source" and not lower_name.endswith((".tar.gz", ".tgz", ".tar")):
            logger.debug("Запись %s не является LaTeX архивом", record.get("article_id"))
            return False
        archive_bytes = self._download_archive(object_name)
        if not archive_bytes:
            self._move_to_error_queue(record, "archive_missing")
            return False

        text, abstract_chunks = self._extract_text_from_archive(archive_bytes)
        if not text:
            self._move_to_error_queue(record, "text_empty")
            return False

        main_text, references_text = self._split_references_block(text)
        citations = self._extract_citations(references_text or text)
        abstract_text = abstract_chunks[0] if abstract_chunks else ""

        parsed_object_name = self._build_parsed_filename(record)
        payload = {
            "article_id": record.get("article_id"),
            "db_id": record.get("db_id"),
            "pdf_path": object_name,
            "parsed_path": parsed_object_name,
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "extraction_method": "latex_source",
            "text": main_text,
            "references_text": references_text,
            "abstract": abstract_text,
            "citations": citations,
        }

        if not self._save_parsed_text(parsed_object_name, payload):
            self._move_to_error_queue(record, "parsed_upload_failed")
            return False

        if stream_id:
            self._remove_stream_record(stream_id)
        self._publish_postprocess_record(record, parsed_object_name)
        self.processed_count += 1
        logger.info(
            "Обработан LaTeX архив %s (строк %d)",
            record.get("article_id") or object_name,
            len(text.splitlines()),
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
