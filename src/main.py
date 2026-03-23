from __future__ import annotations

import logging
import os
import json
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
import yaml

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("ml-service")

from pipeline_core.manager import PipelineManager
from ProcessingPipeline.Fetcher import ArxivFetcher
from service_lib import ServiceConnectionFactory

app = FastAPI(title="ML Service")


def _build_service_clients():
    logger.info("Initializing service clients via ServiceConnectionFactory")
    redis_client = ServiceConnectionFactory.getRedisClient()
    minio_client = ServiceConnectionFactory.getMinioClient()
    db_conn = ServiceConnectionFactory.createDatabaseConnection()
    logger.info("Service clients initialized")
    return redis_client, minio_client, db_conn


def _resolve_ingress_stream(profile: str | None, config_path: str) -> str:
    selected_profile = profile or os.getenv("PIPELINE_PROFILE")
    fallback = "pipeline_ingress_v2"
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file) or {}
        if not isinstance(config, dict):
            return fallback
        effective_profile = selected_profile or config.get("default_profile")
        profiles = config.get("profiles") or {}
        profile_cfg = profiles.get(effective_profile) if isinstance(profiles, dict) else None
        streams = profile_cfg.get("streams") if isinstance(profile_cfg, dict) else None
        ingress = streams.get("ingress") if isinstance(streams, dict) else None
        return str(ingress or fallback)
    except Exception:
        logger.exception("Failed to resolve ingress stream from pipeline config; using fallback")
        return fallback


def _build_v2_ingress_payload(record: Dict[str, Any]) -> Dict[str, Any]:
    identifier = record.get("id")
    source_ref = ""
    preferences = record.get("download_preferences")
    preferred = str(record.get("preferred_format") or "source")
    if isinstance(preferences, dict):
        pref_entry = preferences.get(preferred) or preferences.get("source") or preferences.get("pdf")
        if isinstance(pref_entry, dict):
            source_ref = str(pref_entry.get("url") or "")
        elif isinstance(pref_entry, str):
            source_ref = pref_entry
    return {
        "source_type": "hot_arxiv",
        "source_ref": source_ref,
        "article_id": identifier,
        "metadata": record,
    }


try:
    redis_client, minio_client, database_client = _build_service_clients()
except Exception:
    logger.exception("Failed to initialize one or more base clients")
    raise


@app.get("/")
async def root():
    return {"message": "ML Service is running"}


@app.get("/health")
async def health():
    redis_status = "unhealthy"
    minio_status = "unhealthy"
    database_status = "err"

    try:
        redis_client.ping()
        redis_status = "healthy"
    except Exception:
        logger.exception("Health check Redis failed")

    try:
        minio_client.list_buckets()
        minio_status = "healthy"
    except Exception:
        logger.exception("Health check Minio failed")

    try:
        with database_client.cursor() as cursor:
            cursor.execute("SELECT 1")
        database_status = "alive"
    except Exception:
        logger.exception("Health check Database failed")

    overall_status = "healthy" if redis_status == "healthy" and minio_status == "healthy" and database_status == "alive" else "degraded"
    return {
        "status": overall_status,
        "redis": redis_status,
        "minio": minio_status,
        "database": database_status,
    }


@app.post("/pipeline/v2/run-once")
async def pipeline_v2_run_once(profile: str | None = None):
    config_path = os.getenv("PIPELINE_CONFIG_PATH", "./core/pipeline.yaml")
    selected_profile = profile or os.getenv("PIPELINE_PROFILE")
    logger.info(
        "Pipeline run-once requested: profile=%s config_path=%s",
        selected_profile,
        config_path,
    )
    try:
        manager = PipelineManager(config_path=config_path, profile=selected_profile)
        logger.info("PipelineManager initialized: profile=%s", manager.profile)
        processed = manager.run_once()
        logger.info("Pipeline run-once finished: processed=%s profile=%s", processed, manager.profile)
        return {"processed": processed, "profile": manager.profile}
    except Exception as exc:
        logger.exception("pipeline_v2_run_once failed")
        raise HTTPException(status_code=500, detail=f"pipeline_v2_run_once_failed: {exc}") from exc


@app.post("/pipeline/v2/run-continuous")
async def pipeline_v2_run_continuous(profile: str | None = None):
    config_path = os.getenv("PIPELINE_CONFIG_PATH", "./core/pipeline.yaml")
    selected_profile = profile or os.getenv("PIPELINE_PROFILE")
    logger.info(
        "Pipeline run-continuous requested: profile=%s config_path=%s",
        selected_profile,
        config_path,
    )
    try:
        manager = PipelineManager(config_path=config_path, profile=selected_profile)
        logger.info("PipelineManager initialized: profile=%s", manager.profile)
        manager.run_continuous()
        logger.info("Pipeline run-continuous finished: profile=%s", manager.profile)
        return {"status": "completed", "profile": manager.profile}
    except Exception as exc:
        logger.exception("pipeline_v2_run_continuous failed")
        raise HTTPException(status_code=500, detail=f"pipeline_v2_run_continuous_failed: {exc}") from exc


@app.post("/pipeline/v2/fetch-arxiv-fixed")
async def pipeline_v2_fetch_arxiv_fixed(
    count: int = 100,
    hours_window: int = 24,
    hours_offset: int = 0,
    days_offset: int = 0,
    days_window: int = 0,
    enqueue: bool = True,
    profile: str | None = None,
):
    if count <= 0:
        raise HTTPException(status_code=400, detail="count must be > 0")
    if count > 5000:
        raise HTTPException(status_code=400, detail="count must be <= 5000")

    config_path = os.getenv("PIPELINE_CONFIG_PATH", "./core/pipeline.yaml")
    ingress_stream = _resolve_ingress_stream(profile, config_path)
    logger.info(
        "ArXiv fixed fetch requested: count=%s hours_window=%s offsets=(h:%s,d:%s) days_window=%s enqueue=%s ingress_stream=%s",
        count,
        hours_window,
        hours_offset,
        days_offset,
        days_window,
        enqueue,
        ingress_stream,
    )

    try:
        fetcher = ArxivFetcher(max_results=count, hours_window=hours_window)
        records = await fetcher.fetch_fixed_count(
            count,
            hours_window=hours_window,
            hours_offset=hours_offset,
            days_offset=days_offset,
            days_window=days_window,
        )
        logger.info("ArXiv fetch completed: fetched=%s", len(records))
    except Exception as exc:
        logger.exception("ArXiv fetch failed")
        raise HTTPException(status_code=500, detail=f"pipeline_v2_fetch_arxiv_fixed_failed: {exc}") from exc

    if not enqueue:
        return {
            "fetched": len(records),
            "enqueued": 0,
            "ingress_stream": ingress_stream,
            "sample_ids": [record.get("id") for record in records[:10]],
        }

    enqueued = 0
    for record in records:
        payload = _build_v2_ingress_payload(record)
        redis_client.xadd(ingress_stream, {"data": json.dumps(payload, ensure_ascii=False)})
        enqueued += 1
    logger.info("ArXiv records enqueued: enqueued=%s stream=%s", enqueued, ingress_stream)
    return {
        "fetched": len(records),
        "enqueued": enqueued,
        "ingress_stream": ingress_stream,
        "sample_ids": [record.get("id") for record in records[:10]],
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
