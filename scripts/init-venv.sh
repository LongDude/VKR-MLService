# Создаем виртуальное окружение
ROOT="$(dirname "$(dirname "$(realpath "$0")")")"
cd $ROOT

test -f $ROOT/.venv-lin/bin/activate || python3.11 -m venv $ROOT/.venv-lin
source $ROOT/.venv-lin/bin/activate

# Установка основного стека библиотек.
pip install --upgrade pip setuptools wheel

pip install -r $ROOT/core/requirements.txt
echo "Installation complete!"