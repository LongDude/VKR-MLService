FROM python:3.11-slim
WORKDIR /

# Стандартные утилиты и update
RUN apt-get update --yes && \
    apt-get upgrade --yes --no-install-recommends \
    git wget curl
# RUN apt-get install --yes --no-install-recommends \
    # software-properties-common

# Work dependencies
RUN apt-get update --yes && apt-get install --yes --no-install-recommends \
    postgresql libpq-dev

# Чистка
RUN apt-get clean && rm -rf /var/lib/apt/lists/* && \
    echo "en_US.UTF-8 UTF-8" > /etc/locale.gen

WORKDIR /app