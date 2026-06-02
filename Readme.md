# VKR MLService

MLService предоставляет API, Redis worker и операторские CLI-команды для загрузки,
проверки и ML-обработки научных данных.

## CLI

CLI разделен на четыре категории:

| Категория | Назначение |
| --- | --- |
| `tasks` | Постановка Redis-задач, просмотр cooldown, shutdown worker и восстановление failed queue |
| `worker` | Запуск фонового Redis worker |
| `data` | Проверка и исправление данных, синхронизация cluster read-model, инициализация Qdrant |
| `ml` | Локальный запуск pipeline-операций без постановки Redis-задач |

Для запуска CLI через контейнер используйте `scripts/cli.ps1` на Windows или
`scripts/cli.sh` на Linux/macOS. Скрипты проксируют аргументы в уже работающий
контейнер `vkr-ml-api`, где зависимости установлены в системное Python-окружение.
Имя контейнера можно переопределить через `ML_CONTAINER`.

```powershell
.\scripts\cli.ps1 tasks --help
.\scripts\cli.ps1 worker run --help
.\scripts\cli.ps1 data validate-local-data --help
.\scripts\cli.ps1 ml index-papers --help
```

```sh
sh scripts/cli.sh tasks --help
```

Локальные `scripts/run-cli.ps1` и `scripts/run-cli.sh` остаются dev-обертками для
запуска через `.win-dev`, `.venv-lin` или другое локальное venv.

## Запуск Контура

Make используется для сборки и запуска Docker-контура:

| Команда | Режим |
| --- | --- |
| `make init` | Сборка образов |
| `make up-api` | API и базовая инфраструктура |
| `make up-worker` | API, worker и инфраструктура worker |
| `make up-full` | API, worker и инструменты мониторинга |

## Карта Операций

`tasks` является единственной точкой постановки Redis-задач:

```text
enqueue-paper-indexing
enqueue-keyword-extraction
enqueue-research-entities
enqueue-topic-stats
enqueue-bootstrap-papers
enqueue-cluster-recompute
enqueue-cluster-dynamics
enqueue-topic-reports
enqueue-user-profiles
openalex-cooldown-status
request-worker-shutdown
restore-failed
```

Для `enqueue-cluster-recompute` доступны явные topic scopes и выборка по периоду
через `--period-selected --date-from ... --date-to ...`. Для
`enqueue-cluster-dynamics` доступны cluster/topic scopes и режим
`--cached-clusters`.

`ml` запускает pipeline локально:

```text
index-papers
extract-keywords
index-research-entities
recompute-trends
recompute-cluster-dynamics
generate-topic-report
recompute-user-profile
```

`data` отвечает за контроль и исправление данных:

```text
validate-local-data
rebuild-openalex-yearly-topic-stats
check-openalex-jan1-anomalies
analyze-coverage
analyze-sample-month-coverage
sync-clusters-db
init-qdrant
```

Точные параметры каждой операции доступны через `<command> --help`.

## Конфигурация

Безопасные значения по умолчанию находятся в `config/default.toml`. Секреты
хранятся только в env. Внешний TOML можно выбрать через `ML_CONFIG_FILE` или
`--config-file`, env-файл - через `ML_ENV_FILE` или `--env-file`.

Приоритет значений:

```text
CLI override > process env > .env > external TOML > config/default.toml > built-in defaults
```

Инфраструктурные env-переменные сохраняют прежние имена: `DATABASE_URL`,
`POSTGRES_*`, `REDIS_*`, `QDRANT_*`, `LMSTUDIO_BASE_URL`, `OPENALEX_*`,
`ML_INTERNAL_API_TOKEN`. Для runtime worker используются `ML_WORKER_*`.

API получает settings через LRU cache до перезапуска процесса. CLI и worker
загружают settings один раз на запуск; параметры отдельного вызова могут
переопределять глобальные defaults.

## Контейнерный Worker

Worker в `docker-compose.yml` запускается минимальной командой:

```text
python -m cli.worker run
```

Контейнерные отличия задаются env:

```text
ML_WORKER_SHOW_PROGRESS=false
ML_WORKER_EVENT_REDIS=true
```

Проверка compose-конфигурации:

```sh
docker compose --profile worker config
```

## Breaking Rollout

Redis compatibility aliases удалены. API и worker нужно обновлять одновременно.
Перед обновлением остановите producers и worker, затем очистите рабочие очереди,
failed/pending/admin task keys, cooldown, shutdown, heartbeat и dedupe keys.

Для выделенного ML Redis DB допустим `FLUSHDB`. Для общего Redis удаляйте только
ключи MLService, включая `queue:*`, `admin:data_coverage:*`, `ml:dedupe:*`,
`ml:worker:*` и OpenAlex cooldown/pending keys.

После очистки запускайте инфраструктуру, API, worker и только затем producers.
