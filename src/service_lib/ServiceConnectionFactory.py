from __future__ import annotations

import logging
from os import getenv
from urllib.parse import urlparse

from dotenv import load_dotenv
from minio import Minio
from psycopg2 import connect
from redis import Redis

logger = logging.getLogger(__name__)

ENV_PATH = "./core/.env"
if load_dotenv(ENV_PATH):
    logger.info("Loaded environment from %s", ENV_PATH)
else:
    logger.warning("Environment file not found or not loaded: %s", ENV_PATH)


class ServiceConnectionFactory:
    @staticmethod
    def getRedisClient() -> Redis:
        """Build Redis client from env vars."""
        redis_url = getenv("REDIS_URL", "").strip()
        if redis_url:
            logger.info("Redis client configured from REDIS_URL")
            return Redis.from_url(redis_url, decode_responses=True)

        redis_host = getenv("REDIS_HOST", "redis")
        redis_port = int(getenv("REDIS_PORT", "6379"))
        redis_password = getenv("REDIS_PASSWORD")
        logger.info("Redis client configured from host/port: host=%s port=%s", redis_host, redis_port)
        return Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True,
        )

    @staticmethod
    def getDatabaseDSN() -> str:
        """Build Postgres DSN from env vars."""
        db_dsn = getenv("DATABASE_DSN", "").strip()
        if db_dsn:
            logger.info("Database DSN configured from DATABASE_DSN")
            return db_dsn

        pg_user = getenv("POSTGRES_USER", "user")
        pg_password = getenv("POSTGRES_PASSWORD", "pass")
        pg_host = getenv("POSTGRES_HOST", "vkr-database")
        pg_port = getenv("POSTGRES_PORT", "5432")
        pg_db = getenv("POSTGRES_DB", "db")
        logger.info("Database DSN configured from host/port: host=%s port=%s db=%s", pg_host, pg_port, pg_db)
        return f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"

    @staticmethod
    def createDatabaseConnection():
        """Create a new Postgres connection."""
        dsn = ServiceConnectionFactory.getDatabaseDSN()
        logger.info("Opening new database connection")
        return connect(dsn)

    @staticmethod
    def getMinioClient() -> Minio:
        """Build Minio client from env vars."""
        minio_url = getenv("MINIO_URL", "").strip()
        access_key = getenv("MINIO_ACCESS_KEY") or getenv("MINIO_ROOT_USER")
        secret_key = getenv("MINIO_SECRET_KEY") or getenv("MINIO_ROOT_PASSWORD")

        if minio_url:
            parsed = urlparse(minio_url)
            endpoint = parsed.netloc or parsed.path
            secure = parsed.scheme.lower() == "https"
            logger.info("Minio client configured from MINIO_URL: endpoint=%s secure=%s", endpoint, secure)
            return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

        endpoint = getenv("MINIO_ENDPOINT", "minio:9000")
        secure = getenv("MINIO_SECURE", "false").lower() == "true"
        logger.info("Minio client configured from endpoint: endpoint=%s secure=%s", endpoint, secure)
        return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("ServiceConnectionFactory diagnostics start")
    try:
        redis_client = ServiceConnectionFactory.getRedisClient()
        redis_client.ping()
        logger.info("Redis ping OK")
    except Exception:
        logger.exception("Redis ping failed")

    try:
        minio_client = ServiceConnectionFactory.getMinioClient()
        _ = minio_client.list_buckets()
        logger.info("Minio list_buckets OK")
    except Exception:
        logger.exception("Minio check failed")

    try:
        conn = ServiceConnectionFactory.createDatabaseConnection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        conn.close()
        logger.info("Database probe OK")
    except Exception:
        logger.exception("Database check failed")
