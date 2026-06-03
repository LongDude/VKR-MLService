COMPOSE := docker compose
CLI ?= sh scripts/cli.sh
QDRANT_VECTOR_SIZE ?= 1024
QDRANT_COLLECTION ?= all
QDRANT_DISTANCE ?= Cosine

.DEFAULT_GOAL := help

.PHONY: help init up-dev up-prod down build logs ps up-api up-worker up-monitoring up-full qdrant-init init-qdrant

help:
	@echo Available commands:
	@echo   make up-dev        - Start ML API, worker, and monitoring tools
	@echo   make up-prod       - Build and start ML API with worker
	@echo   make up-api        - Start ML API and base infrastructure only
	@echo   make up-worker     - Start ML API, Redis, worker, and worker infrastructure
	@echo   make up-monitoring - Start ML API and monitoring tools
	@echo   make up-full       - Start ML API, worker, and monitoring tools
	@echo   make init-qdrant   - Initialize Qdrant collections via scripts/cli.sh data init-qdrant
	@echo   make down          - Stop MLService containers
	@echo   make build         - Build MLService Docker images
	@echo   make logs          - Follow MLService container logs
	@echo   make ps            - Show MLService container status
	@echo   CLI wrapper: CLI='sh scripts/cli.sh'
	@echo   Qdrant options: QDRANT_VECTOR_SIZE=1024 QDRANT_COLLECTION=all QDRANT_DISTANCE=Cosine

up-dev: export LOG_LEVEL=DEBUG
up-dev:
	$(COMPOSE) --profile worker --profile monitoring up -d

up-prod: export LOG_LEVEL=INFO
up-prod:
	$(COMPOSE) --profile worker up -d --build

down:
	$(COMPOSE) down --remove-orphans

build:
	$(COMPOSE) build

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

up-api:
	$(COMPOSE) up -d

up-worker:
	$(COMPOSE) --profile worker up -d

up-monitoring:
	$(COMPOSE) --profile monitoring up -d

up-full:
	$(COMPOSE) --profile worker --profile monitoring up -d

init-qdrant:
	$(CLI) data init-qdrant $(QDRANT_VECTOR_SIZE) --collection $(QDRANT_COLLECTION) --distance $(QDRANT_DISTANCE)
