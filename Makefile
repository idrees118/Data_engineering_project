.DEFAULT_GOAL := help
SHELL := /bin/bash
export SHOPSTREAM_DATA_DIR ?= $(CURDIR)/data
DBT := cd dbt && dbt

.PHONY: help install lint format typecheck test test-docker test-unit generate ingest compact quality dbt-build report demo dbt-docs clean up down

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:  ## Install the package with dev + dbt extras
	pip install -e ".[dev,dbt]"

lint:  ## Ruff lint + format check
	ruff check .
	ruff format --check .

format:  ## Auto-format
	ruff check . --fix
	ruff format .

typecheck:  ## mypy (strict) on src/
	mypy

test-unit:  ## Fast unit tests
	pytest tests/unit -q

test:  ## Unit + end-to-end tests (runs dbt; skips tests that need Docker)
	pytest -m "not docker" --cov=shopstream --cov-report=term-missing

test-docker:  ## Tests against real Redpanda and MinIO containers (needs Docker)
	pytest -m docker -v

generate:  ## Simulate 14 days of traffic into the stream
	shopstream generate

ingest:  ## Stream -> bronze Parquet
	shopstream ingest

compact:  ## Merge small bronze files (one file per partition)
	shopstream compact

quality:  ## Bronze quality gates
	shopstream quality

dbt-build:  ## Build and test silver/gold models
	mkdir -p $(SHOPSTREAM_DATA_DIR)/warehouse
	$(DBT) build --profiles-dir .

report:  ## Print headline numbers from the marts
	shopstream report

demo: clean generate ingest quality dbt-build report  ## Run the whole pipeline from scratch (no Docker needed)

dbt-docs:  ## Generate and serve dbt docs + lineage graph
	$(DBT) docs generate --profiles-dir . && $(DBT) docs serve --profiles-dir .

clean:  ## Remove generated data
	rm -rf $(SHOPSTREAM_DATA_DIR) dbt/target dbt/logs

up:  ## Start Redpanda + console
	docker compose up -d redpanda redpanda-console topic-init

down:  ## Stop the stack and remove volumes
	docker compose --profile jobs down -v
