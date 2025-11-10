# init-venv.ps1 - Скрипт инициализации виртуального окружения на Windows

param(
    [string]$PythonPath = "python",
    [string]$RequirementsPath = "core/requirements.txt"
)

Write-Host "Инициализация виртуального окружения..." -ForegroundColor Green

# Проверяем существование requirements.txt
if (-not (Test-Path $RequirementsPath)) {
    Write-Host "Ошибка: Файл $RequirementsPath не найден!" -ForegroundColor Red
    exit 1
}

# Создаем виртуальное окружение
Write-Host "Создание виртуального окружения..." -ForegroundColor Yellow
& $PythonPath -m venv .venv

# Активируем и устанавливаем зависимости
Write-Host "Установка зависимостей..." -ForegroundColor Yellow
.\.venv\Scripts\pip.exe install --upgrade pip
.\.venv\Scripts\pip.exe install -r $RequirementsPath

Write-Host "Виртуальное окружение успешно инициализировано!" -ForegroundColor Green
Write-Host "Для активации выполните: .\.venv\Scripts\Activate.ps1" -ForegroundColor Cyan