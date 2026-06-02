COMPOSE = docker compose

.PHONY: help init up-api up-worker up-full

help:
	@echo '	init      - Build service images'
	@echo '	up-api    - Start API and its base infrastructure'
	@echo '	up-worker - Start API, worker, and worker infrastructure'
	@echo '	up-full   - Start API, worker, and monitoring tools'

init:
	$(COMPOSE) build

up-api:
	$(COMPOSE) up -d

up-worker:
	$(COMPOSE) --profile worker up -d

up-full:
	$(COMPOSE) --profile worker --profile monitoring up -d
