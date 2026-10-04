"""The lake on an S3-compatible object store, using an in-process mock S3 server (moto).

Writes and listing go through pyarrow.fs and run everywhere. Reads by DuckDB (quality gates and
dbt) need DuckDB's `httpfs` extension, which is downloaded on first use; where that is not
possible (offline machines) those tests skip, and they run in CI.
"""

from __future__ import annotations

import socket
from collections.abc import Iterator
from pathlib import Path

import boto3
import duckdb
import pytest
from moto.server import ThreadedMotoServer

from shopstream.quality import run_bronze_checks
from shopstream.storage import Lake, S3Options
from tests.integration.s3_support import dbt_build_on_s3, ingest_simulation

pytestmark = pytest.mark.integration

BUCKET = "shopstream-lake"
CREDS = {"access_key_id": "test", "secret_access_key": "test"}


@pytest.fixture(scope="module")
def s3_endpoint() -> Iterator[str]:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    endpoint = f"http://127.0.0.1:{port}"
    boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ).create_bucket(Bucket=BUCKET)
    yield endpoint
    server.stop()


@pytest.fixture
def lake(s3_endpoint: str, request: pytest.FixtureRequest) -> Lake:
    prefix = f"bronze-{request.node.name}"
    return Lake(f"s3://{BUCKET}/{prefix}", S3Options(s3_endpoint, **CREDS))


def test_lake_round_trip_on_s3(lake: Lake) -> None:
    import pyarrow as pa

    table = pa.table({"a": [1, 2, 3]})
    path = lake.write_parquet(table, "events/event_type=x/ingest_date=2025-01-01/batch-1.parquet")
    lake.write_parquet(table, "events/event_type=x/ingest_date=2025-01-01/batch-2.parquet")

    assert lake.files("events") == sorted([path, path.replace("batch-1", "batch-2")])
    assert lake.read_parquet(path).to_pydict() == {"a": [1, 2, 3]}
    lake.delete([path])
    assert len(lake.files("events")) == 1
    assert not lake.has_files("dead_letter")


def test_ingestion_writes_partitioned_objects_to_the_bucket(s3_endpoint: str, lake: Lake) -> None:
    valid, _ = ingest_simulation(lake)
    client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    keys = [
        o["Key"]
        for o in client.list_objects_v2(Bucket=BUCKET, Prefix=lake.path("").split("/", 1)[1])[
            "Contents"
        ]
    ]
    assert valid > 0
    assert any("/events/event_type=order_placed/ingest_date=" in k for k in keys)
    assert any("/dead_letter/ingest_date=" in k for k in keys)
    assert not any(k.rsplit("/", 1)[-1].startswith(".") for k in keys), "no temp objects"


def _httpfs_available() -> bool:
    try:
        con = duckdb.connect()
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
    except duckdb.Error:
        return False
    return True


needs_httpfs = pytest.mark.skipif(
    not _httpfs_available(), reason="DuckDB httpfs extension cannot be installed here"
)


@needs_httpfs
def test_quality_gates_read_bronze_from_s3(lake: Lake) -> None:
    ingest_simulation(lake)
    results = run_bronze_checks(lake, max_dead_letter_ratio=0.05, max_freshness_hours=None)
    assert all(r.passed for r in results), results


@needs_httpfs
def test_dbt_builds_from_an_s3_lake(s3_endpoint: str, lake: Lake, tmp_path: Path) -> None:
    valid, _ = ingest_simulation(lake)
    dbt_build_on_s3(lake, s3_endpoint, "test", "test", tmp_path)
    con = duckdb.connect(str(tmp_path / "warehouse" / "shopstream.duckdb"), read_only=True)
    try:
        assert con.sql("select count(*) from staging.stg_events").fetchone()[0] == valid  # type: ignore[index]
    finally:
        con.close()
