"""Hourly ShopStream pipeline: ingest -> bronze quality gate -> dbt build -> report.

Each task is a thin shell-out to the same commands a developer runs locally (see Makefile), so
the DAG contains scheduling and failure policy only, never business logic.

The quality gate sits *between* ingestion and modelling on purpose: if the stream is unhealthy
(too many rejects, stale data) the run stops before bad data reaches the marts.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "{{ var.value.get('shopstream_dbt_dir', '/opt/shopstream/dbt') }}"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="shopstream_pipeline",
    description="Stream ingestion into bronze, quality gates, dbt silver/gold build.",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,  # the ingest checkpoint and the DuckDB file are single-writer
    default_args=default_args,
    tags=["shopstream", "lakehouse"],
) as dag:
    ingest = BashOperator(task_id="ingest_stream_to_bronze", bash_command="shopstream ingest")

    quality_gate = BashOperator(task_id="bronze_quality_gate", bash_command="shopstream quality")

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build --profiles-dir .",
    )

    report = BashOperator(
        task_id="publish_report",
        bash_command="shopstream report",
        trigger_rule="all_success",
    )

    ingest >> quality_gate >> dbt_build >> report
