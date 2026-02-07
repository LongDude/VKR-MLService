# Базовый контейнер для запуска проектов с ИИ.
# Собран в целях запуска локальной модели DeepSeek-OCR.
# Использованы следущие версии библиотек
# 1. Cuda 12.8.1 (на базе ОС ubuntu 24.04)
# 2. Python 3.11
# 3. Torch 2.11 (совместимый с cu128)
# 4. Transformers 4.46.3 (проверенная версия)

# Разработан для работы с видеокартами Nvidia 5060Ti:
#   Compute capability: 12.0 (sm_120)
#   Nvidia driver: 575.x или новее

FROM nvidia/cuda:12.8.1-devel-ubuntu24.04
WORKDIR /

# Стандартные утилиты и update
RUN apt-get update --yes && \
    apt-get upgrade --yes --no-install-recommends \
    git wget curl
RUN apt-get install --yes --no-install-recommends \
    software-properties-common

# Python 3.11 
RUN add-apt-repository 'ppa:deadsnakes/ppa'
RUN apt-get update && apt-get install --yes --no-install-recommends\
    python3.11 python3.11-venv python3.11-dev python3-pip

# Чистка
RUN apt-get clean && rm -rf /var/lib/apt/lists/* && \
    echo "en_US.UTF-8 UTF-8" > /etc/locale.gen

# Создаем виртуальное окружение
RUN python3.11 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV VIRTUAL_ENV="/opt/venv"

# Установка основного стека библиотек.
RUN pip install --upgrade pip setuptools wheel

# Torch для Python3.11+cuda 12.8
RUN pip install \
--index-url https://download.pytorch.org/whl/nightly/cu128 \
--pre torch \
torchvision \
torchaudio

# Стабильная версия Transformers и остальные зависимости
RUN pip install \
transformers==4.46.3 \
accelerate>=0.30.0 \
addict \
easydict \
einops

# Сохраняем списком установленные зависимости
RUN /opt/venv/bin/pip freeze > /opt/requirements-base.txt