# Создаем виртуальное окружение
ROOT="$(dirname "$(dirname "$(realpath "$0")")")"
cd $ROOT

test -f $ROOT/.venv-lin/bin/activate || python3.11 -m venv $ROOT/.venv-lin
source $ROOT/.venv-lin/bin/activate

# Установка основного стека библиотек.
pip install --upgrade pip setuptools wheel

# Torch для Python3.11+cuda 12.8
pip install \
--pre torch \
torchvision \
torchaudio \
--index-url https://download.pytorch.org/whl/nightly/cu128

# Стабильная версия Transformers и остальные зависимости
pip install \
'transformers==4.46.3' \
'accelerate>=0.30.0' \
addict \
easydict \
einops

pip install -r $ROOT/core/requirements.txt