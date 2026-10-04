from __future__ import annotations

import os
from pathlib import Path

import pytest

if os.environ.get("REQUIRE_AIRFLOW"):
    import airflow  # noqa: F401  # in the CI job that tests DAGs, a missing install must fail
else:
    pytest.importorskip("airflow", reason="apache-airflow is not installed")

DAG_FILE = Path(__file__).resolve().parents[2] / "orchestration" / "dags" / "shopstream_pipeline.py"


def _load():  # type: ignore[no-untyped-def]
    # `.dags` is read from the parsed files; `get_dag()` would need an initialised metadata DB.
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)


def test_dag_imports_cleanly() -> None:
    assert not _load().import_errors


def test_quality_gate_runs_between_ingest_and_dbt() -> None:
    dag = _load().dags["shopstream_pipeline"]
    assert dag.max_active_runs == 1
    assert dag.get_task("bronze_quality_gate").upstream_task_ids == {"ingest_stream_to_bronze"}
    assert dag.get_task("dbt_build").upstream_task_ids == {"bronze_quality_gate"}


def test_compaction_runs_daily_as_its_own_dag() -> None:
    dag = _load().dags["shopstream_compaction"]
    assert dag.max_active_runs == 1
    assert [t.task_id for t in dag.tasks] == ["compact_bronze"]
