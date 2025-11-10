#!/bin/bash
sleep 10

# Настройка клиента MinIO
mc alias set myminio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}

# Создание бакетов
mc mb myminio/pdf-raw --ignore-existing
mc mb myminio/parsed-text --ignore-existing  
mc mb myminio/ml-models --ignore-existing
mc mb myminio/temp-files --ignore-existing

# Политики доступа
mc anonymous set download myminio/pdf-raw
mc anonymous set download myminio/parsed-text

# Настройка lifecycle правил (удаление временных файлов через 7 дней)
mc ilm add myminio/temp-files --expiry-days "7"