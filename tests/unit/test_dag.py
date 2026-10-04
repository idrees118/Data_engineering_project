from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="apache-airflow is not installed")

DAG_FILE = Path(__file__).resolve().parents[2] / "orchestration" / "dags" / "shopstream_pipeline.py"


def _load():  # type: ignore[no-untyped-def]
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)


def test_dag_imports_cleanly() -> None:
    assert not _load().import_errors


def test_quality_gate_runs_between_ingest_and_dbt() -> None:
    dag = _load().get_dag("shopstream_pipeline")
    assert dag.max_active_runs == 1
    assert dag.get_task("bronze_quality_gate").upstream_task_ids == {"ingest_stream_to_bronze"}
    assert dag.get_task("dbt_build").upstream_task_ids == {"bronze_quality_gate"}


def test_compaction_runs_daily_as_its_own_dag() -> None:
    dag = _load().get_dag("shopstream_compaction")
    assert dag.max_active_runs == 1
    assert [t.task_id for t in dag.tasks] == ["compact_bronze"]
