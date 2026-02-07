from redis import Redis
from minio import Minio
from psycopg2 import connect
from dotenv import load_dotenv
from os import getcwd, getenv

# Предварительная прогрузка при импорте
print("Импортирован класс ServiceConnectionFactory")
if load_dotenv("./core/.env"): 
    print("INFO [ServiceConnectionFactory]: loaded dotenv")
else:
    raise Exception(f"ERR: Ошибка загрузки .env из рабочей директории: {getcwd()}")

class ServiceConnectionFactory(object):
    @staticmethod
    def getRedisClient():
        ''' Инициализирует Redis клиент из параметров в .env '''
        redis_url = getenv("REDIS_URL")

        if redis_url: 
            return Redis.from_url(redis_url)
        else:
            redis_host = getenv("REDIS_HOST", "redis")
            redis_port = getenv("REDIS_PORT", "6379")
            redis_password = getenv("REDIS_PASSWORD")
     
            # redis_auth = f":{redis_password}@" if redis_password else ""
            # redis_url = f"redis://{redis_auth}{redis_host}:{redis_port}/0"
    
            return Redis(
                host=redis_host,
                port=redis_port,
                password=redis_password,
                decode_responses=True
            )

    @staticmethod
    def getDatabaseDSN():
        ''' Собирает из .env DNS для соединения с базой данных '''
        DB_DSN = getenv("DATABASE_DSN")
        if not DB_DSN:
            pg_user = getenv("POSTGRES_USER", "user")
            pg_password = getenv("POSTGRES_PASSWORD", "pass")
            pg_host = getenv("POSTGRES_HOST", "vkr-database")
            pg_port = getenv("POSTGRES_PORT", "5432")
            pg_db = getenv("POSTGRES_DB", "db")
            DB_DSN = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
        return DB_DSN 
    
    @staticmethod
    def createDatabaseConnection():
        ''' Инициализирует и возвращает новое соединение с базой данных '''
        return connect(ServiceConnectionFactory.getDatabaseDSN())

    @staticmethod
    def getMinioClient():
        ''' Инициализирует Minio клиент из параметров в .env '''
        minio_url = getenv("MINIO_URL")
        if not minio_url:
            minio_endpoint = getenv("MINIO_ENDPOINT")
            minio_access = getenv("MINIO_ACCESS_KEY")
            minio_secret = getenv("MINIO_SECRET_KEY")
            minio_secure = False # *: есть возможность расширить конфигурацию .conf файлом
        print(f"INFO: Minio Client: {minio_access} {minio_secret}")
        return Minio(
            minio_endpoint,
            access_key=minio_access,
            secret_key=minio_secret,
            secure=minio_secure
        )

if __name__ == "__main__":
    print("Служебный класс для создания соединения с внутренними сервисами")