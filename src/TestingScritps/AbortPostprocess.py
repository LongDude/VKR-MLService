from minio import Minio
import logging
import redis
import json
from service_lib import ServiceConnectionFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PREPROCESS_QUEUE_KEY = "preprocess_queue"
POSTPROCESS_QUEUE = "postprocess_queue"

redis_client = ServiceConnectionFactory.getRedisClient()
stream_err_data = redis_client.xrange(POSTPROCESS_QUEUE)
if not stream_err_data:
    print("No err keys found")

count = 0
for st_id, payload in stream_err_data:
    parsed = {key: json.loads(value) for key, value in payload.items()}
    record: dict = parsed.get("data")

    link_object = {
        "article_id": record["article_id"],
        "db_id": record["db_id"],
        "pdf_path": record["pdf_path"],
        "download_format": record["download_format"],
    }

    payload = json.dumps(link_object, ensure_ascii=False)
    redis_client.xadd(PREPROCESS_QUEUE_KEY, {"data": payload})
    redis_client.xdel(POSTPROCESS_QUEUE, st_id)
    count += 1
print(f"Изменено {count} записей")