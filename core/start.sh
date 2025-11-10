#!/bin/sh

echo "Ожидание доступности Redis..."
while ! nc -z redis 6379; do
  sleep 1
done

echo "Ожидание доступности MinIO..."
while ! nc -z minio 9000; do
  sleep 1
done

echo "Все зависимости доступны, запуск приложения..."
exec /app/.venv/bin/python core/main.py