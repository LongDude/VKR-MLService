FROM python:3.11-alpine3.18 as builder
WORKDIR /app

# Устанавливаем системные зависимости для сборки
RUN apk update && apk add --no-cache \
    postgresql-dev \
    gcc \
    python3-dev \
    musl-dev \
    libffi-dev

# Копируем requirements и устанавливаем зависимости
COPY core/requirements.txt .

RUN python -m venv /app/.venv && \
    /app/.venv/bin/pip install --upgrade pip && \
    /app/.venv/bin/pip install --no-cache-dir -r requirements.txt

# Финальный образ
FROM python:3.11-alpine3.18
WORKDIR /app

# Устанавливаем только runtime зависимости
RUN apk update && apk add --no-cache \
    postgresql-libs \
    libpq

# Копируем venv из builder stage
COPY --from=builder /app/.venv /app/.venv

# Копируем исходный код
COPY src/ /app/src/
COPY core/ /app/core/
COPY ./main.py /app/main.py

# Используем Python из venv
CMD ["/app/.venv/bin/python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]