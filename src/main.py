# core/main.py
from fastapi import FastAPI
from fastapi import HTTPException
import redis
from minio import Minio
from psycopg2 import connect, extensions
from dotenv import load_dotenv
import os
from pipeline_core.manager import PipelineManager

app = FastAPI(title="ML Service")
load_dotenv("./core/.env")

# Инициализация клиентов
redis_client = redis.Redis(
    host='redis',
    port=6379,
    password=os.getenv('REDIS_PASSWORD', 'redis123'),
    decode_responses=True
)

minio_client = Minio(
    'minio:9000',
    access_key=os.getenv('MINIO_ROOT_USER', 'admin'),
    secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'password123'),
    secure=False
)

database_client = connect(
    host="vkr-database",
    port=5432,
    user=os.getenv('POSTGRES_USER','postgres'),
    password=os.getenv('POSTGRES_PASSWORD', 'gresP0st'),
    database=os.getenv('POSTGRES_DB','vkr-db'),
)

@app.get("/")
async def root():
    return {"message": "ML Service is running"}

@app.get("/health")
async def health():
    # Проверка подключения к Redis
    try:
        redis_client.ping()
        redis_status = "healthy"
    except:
        redis_status = "unhealthy"
    
    # Проверка подключения к MinIO
    try:
        minio_client.list_buckets()
        minio_status = "healthy"
    except:
        minio_status = "unhealthy"
    
    try:
        c = database_client.cursor()
        c.execute('SELECT 1')
        database_status = 'alive'
    except:
        database_status = 'err'

    return {
        "status": "healthy",
        "redis": redis_status,
        "minio": minio_status,
        "database": database_status
    }


@app.post("/pipeline/v2/run-once")
async def pipeline_v2_run_once(profile: str | None = None):
    config_path = os.getenv("PIPELINE_CONFIG_PATH", "./core/pipeline.yaml")
    selected_profile = profile or os.getenv("PIPELINE_PROFILE")
    try:
        manager = PipelineManager(config_path=config_path, profile=selected_profile)
        processed = manager.run_once()
        return {"processed": processed, "profile": manager.profile}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"pipeline_v2_run_once_failed: {exc}") from exc


@app.post("/pipeline/v2/run-continuous")
async def pipeline_v2_run_continuous(profile: str | None = None):
    config_path = os.getenv("PIPELINE_CONFIG_PATH", "./core/pipeline.yaml")
    selected_profile = profile or os.getenv("PIPELINE_PROFILE")
    try:
        manager = PipelineManager(config_path=config_path, profile=selected_profile)
        manager.run_continuous()
        return {"status": "completed", "profile": manager.profile}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"pipeline_v2_run_continuous_failed: {exc}") from exc

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
