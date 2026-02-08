# Cuda 12.8.1 (на базе ОС ubuntu 24.04)
# Python 3.11
# Torch 2.11 (совместимый с cu128)
# Transformers 4.46.3 (проверенная версия)

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

# Work dependencies
RUN apt-get update --yes && apt-get install --yes --no-install-recommends \
    postgresql

# Чистка
RUN apt-get clean && rm -rf /var/lib/apt/lists/* && \
    echo "en_US.UTF-8 UTF-8" > /etc/locale.gen

WORKDIR /app