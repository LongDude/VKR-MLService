import json
import logging
from typing import Optional, Dict, Any, List, Tuple
import redis
import requests
from minio import Minio
from minio.error import S3Error
import io
from service_lib import ServiceConnectionFactory

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# TODO: дополнительные проверки на корректность записей в БД для сохранения целостности
class Loader:
    """
    Класс для загрузки PDF-файлов статей из ArXive в Minio через Redis Stream
    """
    
    def __init__(
        self,
        redis_download_stream_key: str = 'metadata_queue',
        redis_preprocess_stream_key: str = 'preprocess_queue',
        redis_err_stream_key: str = 'err_queue',
        minio_bucket: str = 'pdf-raw',
        arxiv_pdf_url: str = 'https://arxiv.org/pdf/',
        arxiv_source_url: str = 'https://arxiv.org/src/',
        max_batch_articles: Optional[int] = None,
        max_queue_articles: Optional[int] = None
    ):
        """
        Инициализация Loader
        
        Args:
            redis_download_stream_key: Ключ Redis Stream для потока загрузки
            redis_raw_pdf_stream_key: Ключ Redis Stream для потока обработки
            minio_bucket: Бакет Minio для хранения PDF
            minio_secure: Использовать SSL для Minio
            arxiv_pdf_url: Базовый URL для PDF на arXiv
            max_articles: Максимальное количество статей для обработки (опционально)
        """
        self.redis_download_stream_key = redis_download_stream_key
        self.redis_preprocess_stream_key = redis_preprocess_stream_key
        self.redis_err_stream_key = redis_err_stream_key
        self.minio_bucket = minio_bucket
        self.arxiv_pdf_url = arxiv_pdf_url.rstrip('/')
        self.arxiv_source_url = arxiv_source_url.rstrip('/')
        self.max_batch_articles = max_batch_articles
        self.max_queue_articles = max_queue_articles
        self.processed_count = 0
        
        self.redis_client = ServiceConnectionFactory.getRedisClient()
        self.minio_client = ServiceConnectionFactory.getMinioClient()
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self) -> None:
        """Проверяет существование бакета в Minio, создает если нет"""
        try:
            if not self.minio_client.bucket_exists(self.minio_bucket):
                self.minio_client.make_bucket(self.minio_bucket)
                logger.info(f"Бакет {self.minio_bucket} создан в Minio")
        except S3Error as e:
            logger.error(f"Ошибка при создании бакета: {e}")
            raise
    
    def check_stream_has_records(self) -> bool:
        """
        Проверяет наличие записей в Redis Stream
        
        Returns:
            bool: True если есть записи, False если нет
        """
        try:
            stream_length = self.redis_client.xlen(self.redis_download_stream_key)
            has_records = stream_length > 0
            logger.info(f"Проверка потока: {stream_length} записей найдено")
            return has_records
        except redis.RedisError as e:
            logger.error(f"Ошибка при проверке Redis Stream: {e}")
            return False
    
    def get_first_record(self) -> Optional[Dict[str, Any]]:
        """
        Получает первую запись из Redis Stream
        
        Returns:
            Optional[Dict]: Словарь с данными записи или None если записей нет
        """
        try:
            # Получаем первую запись из потока
            stream_data = self.redis_client.xrange(self.redis_download_stream_key, count=1)
            
            if not stream_data:
                logger.info("В потоке нет записей")
                return None
            
            # Извлекаем данные записи
            stream_id, record_data = stream_data[0]
            record = {key: json.loads(value) for key, value in record_data.items()}.get("data")
            record['stream_id'] = stream_id
            
            logger.info(f"Получена запись: {record.get('id', 'Unknown ID')}")
            return record
            
        except (redis.RedisError, json.JSONDecodeError) as e:
            logger.error(f"Ошибка при получении записи из Redis: {e}")
            return None
    

    def _clean_arxiv_identifier(self, article_id: Optional[str]) -> str:
        if not article_id:
            return ""
        return article_id.split(":")[-1]

    def _build_download_candidates(self, record: Dict[str, Any]) -> List[Dict[str, str]]:
        arxiv_id = record.get("id") or record.get("article_id")
        clean_id = self._clean_arxiv_identifier(arxiv_id)
        preferences = record.get("download_preferences") or {}
        preferred_format = str(record.get("preferred_format") or "source").lower()

        order: List[str] = []
        if preferred_format:
            order.append(preferred_format)
        order.extend(["source", "pdf"])

        final_order: List[str] = []
        seen: set[str] = set()
        for fmt in order:
            if fmt and fmt not in seen:
                seen.add(fmt)
                final_order.append(fmt)

        fallback_urls: Dict[str, str] = {}
        if clean_id:
            fallback_urls["source"] = f"{self.arxiv_source_url}/{clean_id}"
            fallback_urls["pdf"] = f"{self.arxiv_pdf_url}/{clean_id}.pdf"

        candidates: List[Dict[str, str]] = []
        for fmt in final_order:
            pref_entry = preferences.get(fmt) if isinstance(preferences, dict) else None
            url = None
            extension = None
            if isinstance(pref_entry, dict):
                url = pref_entry.get("url")
                extension = pref_entry.get("extension")
            elif isinstance(pref_entry, str):
                url = pref_entry

            if not url:
                url = fallback_urls.get(fmt)
            if not url:
                continue

            if not extension:
                extension = "tar.gz" if fmt == "source" else "pdf"

            candidates.append({"format": fmt, "url": url, "extension": extension})

        if not candidates and fallback_urls:
            for fmt, url in fallback_urls.items():
                candidates.append({"format": fmt, "url": url, "extension": "tar.gz" if fmt == "source" else "pdf"})

        return candidates

    def _download_from_url(self, url: str) -> Optional[Tuple[bytes, str]]:
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            content_type = response.headers.get("content-type", "application/octet-stream")
            return response.content, content_type
        except requests.RequestException as exc:
            logger.error(f"Ошибка скачивания %s: %s", url, exc)
            return None

    def upload_to_minio(self, pdf_data: bytes, object_name: str, content_type: str = 'application/pdf') -> bool:
        """
        Загружает PDF файл в Minio
        
        Args:
            pdf_data: Данные PDF файла
            object_name: Имя объекта в Minio
            
        Returns:
            bool: True если успешно, False при ошибке
        """
        try:
            # Создаем поток из данных PDF
            pdf_stream = io.BytesIO(pdf_data)
            
            # Загружаем в Minio
            res = self.minio_client.put_object(
                bucket_name=self.minio_bucket,
                object_name=object_name,
                data=pdf_stream,
                length=len(pdf_data),
                content_type=content_type or 'application/octet-stream'
            )
            res.location
            
            logger.info(f"PDF успешно загружен в Minio: {object_name}")
            return True
            
        except S3Error as e:
            logger.error(f"Ошибка при загрузке в Minio: {e}")
            return False
    
    def remove_record_from_stream(self, stream_id: str) -> bool:
        """
        Удаляет запись из Redis
        
        Args:
            stream_id: ID записи в Redis Stream
            
        Returns:
            bool: True если успешно удалено
        """
        try:
            self.redis_client.xdel(self.redis_download_stream_key, stream_id)
            logger.info(f"Запись удалена из потока: {stream_id}")
            return True
        except redis.RedisError as e:
            logger.error(f"Ошибка при удалении записи: {e}")
            return False
    
    def redis_save_to_err_stream(self, record_data: Dict[str, Any]) -> bool: 
        """
        Сохраняет сбоившую запись в поток ошибок Redis для дальнешей отладки оператором
        
        Args:
            stream_id: ID записи в Redis Stream
        Returns:
            bool: True если успешно сохранено
        """

        try:
            payload = json.dumps(record_data, ensure_ascii=False)
            self.redis_client.xadd(self.redis_err_stream_key, {"data": payload})
        except redis.RedisError as e:
            logger.error(f"Ошибка при удалении записи: {e}")
            return False



    def redis_save_to_preprocess_stream(
        self,
        paper_pdf_path: str,
        paper_database_idx: int,
        paper_arxive_idx: str,
        download_format: str,
        download_url: Optional[str] = None,
    ):
        """Persist metadata about the downloaded object into preprocess queue."""

        link_object = {
            "article_id": paper_arxive_idx,
            "db_id": paper_database_idx,
            "pdf_path": paper_pdf_path,
            "download_format": download_format,
        }
        if download_url:
            link_object["download_url"] = download_url

        try:
            payload = json.dumps(link_object, ensure_ascii=False)
            self.redis_client.xadd(self.redis_preprocess_stream_key, {"data": payload})
        except redis.RedisError as exc:
            logger.error(f"Failed to push preprocess record: {exc}")
            return False
        return True

    def process_single_article(self) -> bool:
        """
        Обрабатывает одну статью из потока
        
        Returns:
            bool: True если статья успешно обработана, False если нет записей или ошибка
        """

        # Проверяем лимит статей в очереди (жесткий)
        preprocess_queue = self.redis_client.xlen(self.redis_preprocess_stream_key)
        if self.max_queue_articles and preprocess_queue >= self.max_queue_articles:
            logger.info(f"Достигнут лимит загруженных статей в очереди: {self.max_queue_articles}")
            return False
        elif self.max_queue_articles:
            logger.info(f"Размер очереди предобработки: {preprocess_queue}/{self.max_queue_articles}")

        # Получаем первую запись
        record = self.get_first_record()
        if not record:
            return False
        
        db_id = record.get('db_id')
        stream_id = record.get('stream_id')
        article_id = record.get('id')
        
        if not article_id:
            logger.error("Отсутствует ID статьи в записи")
            self.remove_record_from_stream(stream_id)
            self.redis_save_to_err_stream(record)
            return False
        
        try:
            candidates = self._build_download_candidates(record)
            download_blob = None
            chosen_candidate = None

            for candidate in candidates:
                download_blob = self._download_from_url(candidate["url"])
                if download_blob:
                    chosen_candidate = candidate
                    logger.info("Загружен %s формат для статьи %s", candidate["format"], article_id)
                    break

            if not download_blob or not chosen_candidate:
                logger.error("Не удалось скачать материалы для статьи: %s", article_id)
                return False

            file_bytes, content_type = download_blob
            clean_id = self._clean_arxiv_identifier(article_id) or article_id.replace(':', '_')
            extension = chosen_candidate.get("extension") or ("tar.gz" if chosen_candidate["format"] == "source" else "pdf")
            if not extension.startswith('.'):
                extension = f".{extension}"
            object_name = f"{clean_id}{extension}"

            if self.upload_to_minio(file_bytes, object_name, content_type):
                self.remove_record_from_stream(stream_id)
                self.redis_save_to_preprocess_stream(
                    object_name,
                    db_id,
                    article_id,
                    chosen_candidate["format"],
                    chosen_candidate.get("url"),
                )
                self.processed_count += 1
                logger.info("Статья успешно обработана (%s): %s", chosen_candidate["format"], article_id)
                return True
            return False
        except Exception as e:
            logger.error(f"Ошибка при обработке статьи {article_id}: {e}")
            return False
    
    def run(self, continuous: bool = False) -> None:
        """
        Запускает обработку статей
        
        Args:
            continuous: Если True, обрабатывает статьи непрерывно пока они есть
        """
        logger.info("Запуск обработки статей")
        
        if continuous:
            while True:

                # Проверяем лимит статей за сессию (только для последовательной)
                if self.max_batch_articles and self.processed_count >= self.max_batch_articles:
                    logger.info(f"Достигнут лимит статей за сессию: {self.max_batch_articles}")
                    return False
                elif self.max_batch_articles:
                    logger.info(f"Обработка: {self.processed_count+1}/{self.max_batch_articles} статьи за сессию")

                if not self.check_stream_has_records():
                    logger.info("Поток опустел")
                    break
                
                if not self.process_single_article():
                    break
        else:
            # Обрабатываем одну статью
            if self.check_stream_has_records():
                self.process_single_article()
        
        logger.info(f"Обработка завершена. Обработано статей: {self.processed_count}")


# Пример использования для Celery задачи
class CeleryLoaderTask:
    """
    Обертка для использования в Celery
    """
    
    def __init__(self, loader: Loader):
        self.loader = loader
    
    def process_single_article_task(self) -> bool:
        """
        Celery задача для обработки одной статьи
        """
        return self.loader.process_single_article()


# Пример использования
if __name__ == "__main__":
    # Создание экземпляра Loader
    loader = Loader(
        max_batch_articles=30,  # Обработать максимум 30 статей
        max_queue_articles=40
    )
    
    loader.run(continuous=True)
