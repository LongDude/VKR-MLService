import io
import json
import logging
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pdfplumber
import redis
from minio.error import S3Error
from PIL import Image

from service_lib import ServiceConnectionFactory

try:
    import fitz
except ImportError:  # pragma: no cover
    fitz = None  # type: ignore[assignment]

try:
    import pytesseract
except ImportError:  # pragma: no cover
    pytesseract = None  # type: ignore[assignment]

try:
    from deepseek_ocr import DeepSeekOCR, OCRMode
    from deepseek_ocr.exceptions import ConfigurationError as DeepSeekConfigurationError
except ImportError:  # pragma: no cover
    # DeepSeekOCR = None  # type: ignore[assignment]
    OCRMode = None  # type: ignore[assignment]
    DeepSeekConfigurationError = None  # type: ignore[assignment]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PDFProcessor:
    """Process one article from Redis preprocess_queue and dump parsed text to Minio."""

    def __init__(
        self,
        redis_preprocess_stream_key: str = "preprocess_queue",
        redis_postprocess_stream_key: str = "postprocess_queue",
        redis_error_stream_key: str = "preprocess_err_queue",
        raw_pdf_bucket: str = "pdf-raw",
        parsed_text_bucket: str = "parsed-text",
        ocr_page_limit: int = 12,
    ):
        self.redis_preprocess_stream_key = redis_preprocess_stream_key
        self.redis_postprocess_stream_key = redis_postprocess_stream_key
        self.redis_error_stream_key = redis_error_stream_key
        self.raw_pdf_bucket = raw_pdf_bucket
        self.parsed_text_bucket = parsed_text_bucket
        self.processed_count = 0
        self.ocr_page_limit = ocr_page_limit
        self.redis_client = ServiceConnectionFactory.getRedisClient()
        self.minio_client = ServiceConnectionFactory.getMinioClient()
        self.deepseek_client: Optional[DeepSeekOCR] = None
        self._ensure_bucket(self.raw_pdf_bucket)
        self._ensure_bucket(self.parsed_text_bucket)

    def _ensure_bucket(self, bucket_name: str) -> None:
        try:
            if not self.minio_client.bucket_exists(bucket_name):
                self.minio_client.make_bucket(bucket_name)
                logger.info(f"Created missing Minio bucket: {bucket_name}")
        except S3Error as exc:
            logger.error(f"Unable to ensure Minio bucket {bucket_name}: {exc}")
            raise

    def _get_last_record(self) -> Optional[Dict[str, Any]]:
        try:
            entries = self.redis_client.xrevrange(self.redis_preprocess_stream_key, count=1)
            if not entries:
                logger.info("preprocess_queue is empty")
                return None

            stream_id, payload = entries[0]
            parsed = {key: json.loads(value) for key, value in payload.items()}
            record = parsed.get("data")
            if not isinstance(record, dict):
                logger.warning("Skipping malformed stream record (no data)")
                return None
            record["stream_id"] = stream_id
            return record
        except (redis.RedisError, json.JSONDecodeError) as exc:
            logger.error(f"Failed to read preprocess_queue: {exc}")
            return None

    def _remove_stream_record(self, stream_id: str) -> None:
        if not stream_id:
            return
        try:
            self.redis_client.xdel(self.redis_preprocess_stream_key, stream_id)
        except redis.RedisError as exc:
            logger.warning("Failed to remove record %s: %s", stream_id, exc)

    def _publish_stream_payload(self, stream_key: str, payload: Dict[str, Any]) -> None:
        try:
            self.redis_client.xadd(stream_key, {"data": json.dumps(payload, ensure_ascii=False)})
        except redis.RedisError as exc:
            logger.error("Failed to push record to %s: %s", stream_key, exc)

    def _normalize_object_name(self, raw_path: str) -> str:
        if not raw_path:
            return ""
        step = raw_path.strip().lstrip("/")
        if ":" in step and not step.lower().startswith(("http://", "https://", "minio://")):
            step = step.split(":", 1)[-1]
        return step

    def _download_pdf(self, object_name: str) -> Optional[bytes]:
        if not object_name:
            logger.error("Empty pdf_path was supplied, skipping download")
            return None

        response = None
        try:
            response = self.minio_client.get_object(self.raw_pdf_bucket, object_name)
            pdf_bytes = response.read()
            if not pdf_bytes:
                logger.error("Downloaded PDF %s is empty", object_name)
                return None
            return pdf_bytes
        except S3Error as exc:
            logger.error("Failed to fetch %s from Minio bucket %s: %s", object_name, self.raw_pdf_bucket, exc)
            return None
        finally:
            if response:
                try:
                    response.close()
                    response.release_conn()
                except Exception:
                    logger.debug("Failed to release Minio response connection", exc_info=False)

    def _extract_with_pdfplumber(self, pdf_bytes: bytes) -> str:
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                chunks: List[str] = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        chunks.append(text.strip())
            return "\n\n".join(chunks).strip()
        except Exception as exc:
            logger.debug("pdfplumber failed to parse PDF: %s", exc)
            return ""

    def _get_deepseek_client(self) -> Optional[DeepSeekOCR]:
        if self.deepseek_client:
            return self.deepseek_client
        if DeepSeekOCR is None:
            logger.debug("DeepSeekOCR package is not installed")
            return None

        try:
            self.deepseek_client = DeepSeekOCR()
            return self.deepseek_client
        except Exception as exc:  # pylint: disable=broad-except
            reason = exc
            if DeepSeekConfigurationError is not None and isinstance(exc, DeepSeekConfigurationError):
                reason = f"configuration error ({exc})"
            logger.warning("DeepSeek OCR client can't be initialized: %s", reason)
            return None

    def _extract_with_deepseek(self, pdf_bytes: bytes) -> str:
        client = self._get_deepseek_client()
        if not client or OCRMode is None:
            return ""

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = tmp.name

        try:
            text = client.parse(tmp_path, mode=OCRMode.FREE_OCR)
            return text.strip()
        except Exception as exc:
            logger.warning("DeepSeek OCR failed: %s", exc)
            return ""
        finally:
            try:
                Path(tmp_path).unlink()
            except FileNotFoundError:
                pass

    def _extract_with_local_ocr(self, pdf_bytes: bytes) -> str:
        if fitz is None or pytesseract is None:
            logger.debug("Local OCR prerequisites are not available", exc_info=False)
            return ""

        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        except Exception as exc:
            logger.warning("PyMuPDF failed to read PDF: %s", exc)
            return ""

        chunks: List[str] = []
        try:
            for page_index, page in enumerate(doc):
                if page_index >= self.ocr_page_limit:
                    break
                pix = page.get_pixmap(matrix=fitz.Matrix(300 / 72, 300 / 72))
                image = Image.open(io.BytesIO(pix.tobytes("png")))
                image = image.convert("RGB")
                text = pytesseract.image_to_string(image)
                if text:
                    chunks.append(text.strip())
        except Exception as exc:
            logger.warning("Local OCR pipeline failed: %s", exc)
        finally:
            doc.close()

        return "\n\n".join(chunks).strip()

    def _extract_text(self, pdf_bytes: bytes) -> Tuple[str, str]:
        text = self._extract_with_pdfplumber(pdf_bytes)
        if text:
            return text, "pdfplumber"

        text = self._extract_with_deepseek(pdf_bytes)
        if text:
            return text, "deepseek"

        text = self._extract_with_local_ocr(pdf_bytes)
        return text, "pytesseract"

    def _extract_citations(self, text: str) -> List[str]:
        markers = ["references", "bibliography", "литература", "библиография", "список литературы"]
        lower_text = text.lower()
        start = None
        for marker in markers:
            idx = lower_text.find(marker)
            if idx != -1:
                start = idx
                break

        if start is None:
            block = text[-4000:]
        else:
            block = text[start:]

        lines = [line.strip() for line in block.splitlines() if line.strip()]
        citations: List[str] = []
        number_pattern = re.compile(r"^(\[\d+\]|[0-9]+[\.\)]|•|-)")
        for line in lines:
            if number_pattern.match(line):
                citations.append(line)
            elif citations:
                citations[-1] += f" {line}"

        return citations

    def _build_parsed_filename(self, record: Dict[str, Any]) -> str:
        candidate = record.get("pdf_path") or record.get("article_id") or "document"
        candidate = self._normalize_object_name(candidate)
        if candidate.lower().endswith(".pdf"):
            candidate = candidate[: -len(".pdf")]
        sanitized = re.sub(r"[^a-zA-Z0-9_.-]", "_", candidate)
        sanitized = sanitized or "parsed_article"
        return f"{sanitized}.json"

    def _save_parsed_text(
        self,
        object_name: str,
        payload: Dict[str, Any],
    ) -> bool:
        data = json.dumps(payload, ensure_ascii=False, indent=2)
        encoded = data.encode("utf-8")
        try:
            self.minio_client.put_object(
                self.parsed_text_bucket,
                object_name,
                io.BytesIO(encoded),
                length=len(encoded),
                content_type="application/json",
            )
            return True
        except S3Error as exc:
            logger.error("Failed to upload parsed text %s: %s", object_name, exc)
            return False

    def _clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return {key: value for key, value in record.items() if key != "stream_id"}

    def _move_to_error_queue(self, record: Dict[str, Any], reason: Optional[str] = None) -> None:
        stream_id = record.get("stream_id")
        if stream_id:
            self._remove_stream_record(stream_id)
        payload = self._clean_record(record)
        if reason:
            payload["error"] = reason
        self._publish_stream_payload(self.redis_error_stream_key, payload)

    def _publish_postprocess_record(self, record: Dict[str, Any], parsed_path: str) -> None:
        payload = {
            **self._clean_record(record),
            "text_path": parsed_path,
            "original_pdf_path": record.get("pdf_path"),
        }
        self._publish_stream_payload(self.redis_postprocess_stream_key, payload)

    def process_last_record(self) -> bool:
        record = self._get_last_record()
        if not record:
            return False

        stream_id = record.get("stream_id")
        article_id = record.get("article_id")
        pdf_raw_path = record.get("pdf_path", "")
        logger.info("Processing article %s (stream id %s)", article_id, stream_id)
        object_name = self._normalize_object_name(pdf_raw_path)
        pdf_bytes = self._download_pdf(object_name)
        if not pdf_bytes:
            self._move_to_error_queue(record, reason="pdf_missing")
            return False

        text, method = self._extract_text(pdf_bytes)
        if not text:
            self._move_to_error_queue(record, reason="text_empty")
            return False

        citations = self._extract_citations(text)
        parsed_object_name = self._build_parsed_filename(record)
        payload = {
            "article_id": article_id,
            "db_id": record.get("db_id"),
            "pdf_path": pdf_raw_path,
            "parsed_path": parsed_object_name,
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "extraction_method": method,
            "text": text,
            "citations": citations,
        }

        if not self._save_parsed_text(parsed_object_name, payload):
            self._move_to_error_queue(record, reason="parsed_upload_failed")
            return False

        self._remove_stream_record(stream_id)
        self._publish_postprocess_record(record, parsed_object_name)
        self.processed_count += 1
        logger.info("Parsed article %s from %s with %d citations", article_id, method, len(citations))
        return True

    def run(self, continuous: bool = False) -> None:
        logger.info("PDFProcessor started")
        if continuous:
            while True:
                processed = self.process_last_record()
                if not processed:
                    break
        else:
            self.process_last_record()
        logger.info("PDFProcessor stopped after %d articles", self.processed_count)


if __name__ == "__main__":
    processor = PDFProcessor()
    processor.run(continuous=True)
