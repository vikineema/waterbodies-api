#!make
SHELL := /usr/bin/env bash

include  .env

ENV_FILE =  $(abspath .env)
export ENV_FILE

up: ## Bring up your Docker environment
	docker compose up -d 

down:
	docker compose down --remove-orphans

python-env-shell:
	docker compose exec python-env /bin/bash 

add-sample-data:
	docker compose exec python-env /bin/bash -c "python scripts/dump_db_data.py"

db-shell:
	docker compose exec -u postgres db-postgres psql

lint-src:
	ruff check --select I --fix server/        
	ruff format --verbose server/
