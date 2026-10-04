# syntax=docker/dockerfile:1
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SHOPSTREAM_DATA_DIR=/app/data

WORKDIR /app

# Dependencies first so code changes don't invalidate the (slow) install layer.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install ".[kafka,dbt]"

COPY dbt ./dbt
COPY orchestration ./orchestration

RUN useradd --create-home --uid 1000 shopstream \
    && mkdir -p /app/data && chown -R shopstream /app
USER shopstream

ENTRYPOINT ["shopstream"]
