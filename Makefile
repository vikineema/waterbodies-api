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

pg-loader-shell:
	docker compose exec pgloader sh

db-shell:
	docker compose exec -u postgres db-postgres psql

check-tables-in-filedb:
	docker run --rm -it \
    -v ./sample_waterbodies.db:/tmp/sample_waterbodies.db \
    ghcr.io/osgeo/gdal:ubuntu-small-latest \
    ogrinfo /tmp/sample_waterbodies.db

dump-from-sqlite:
	# None spatial data
	docker run --rm -it \
		--network=waterbodies-api_default \
		-v ./sample_waterbodies.db:/tmp/sample_waterbodies.db \
		dimitri/pgloader \
		pgloader /tmp/sample_waterbodies.db \
		"postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):5432/$(POSTGRES_DB)?sslmode=disable"
	
	# Spatial data 
	docker run --rm -it \
		--network=waterbodies-api_default \
		-v ./sample_waterbodies.db:/tmp/sample_waterbodies.db \
		ghcr.io/osgeo/gdal:ubuntu-small-latest \
		ogr2ogr \
		-f "PostgreSQL" \
		"PG:host=$(POSTGRES_HOST) port=5432 dbname=$(POSTGRES_DB) user=$(POSTGRES_USER) password=$(POSTGRES_PASSWORD)" \
		/tmp/sample_waterbodies.db \
		-overwrite \
		-progress