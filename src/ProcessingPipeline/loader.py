from dotenv import load_dotenv
from minio import Minio

ARXIV_BASE_URL = "http://export.arxiv.org/"
minio_client = Minio(
    'minio:9000',
    access_key=os.getenv('MINIO_ROOT_USER', 'admin'),
    secret_key=os.getenv('MINIO_ROOT_PASSWORD', 'password123'),
    secure=False
)


def load_worker():
    pass

if __name__ == "__main__":
    pass