from minio import Minio
import logging
import redis
import json
from service_lib import ServiceConnectionFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PREPROCESS_QUEUE_KEY = "preprocess_queue"
PREPROCESS_ERR_QUEUE = "preprocess_err_queue"

redis_client = ServiceConnectionFactory.getRedisClient()
stream_err_data = redis_client.xrange(PREPROCESS_ERR_QUEUE)
if not stream_err_data:
    print("No err keys found")

count = 0
for st_id, payload in stream_err_data:
    parsed = {key: json.loads(value) for key, value in payload.items()}
    record: dict = parsed.get("data")

    record.pop("error")

    payload = json.dumps(record, ensure_ascii=False)
    redis_client.xadd(PREPROCESS_QUEUE_KEY, {"data": payload})
    redis_client.xdel(PREPROCESS_ERR_QUEUE, st_id)
    count += 1
print(f"Изменено {count} записей")