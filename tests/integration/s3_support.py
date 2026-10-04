"""Helpers shared by the mock-S3 (moto) and real-S3-server (container) test modules."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

from shopstream.generator import SimulationConfig, simulate
from shopstream.ingestion import BronzeWriter, run_ingestion
from shopstream.storage import Lake
from shopstream.streaming.base import Message

REPO = Path(__file__).resolve().parents[2]
DBT = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")


class ListConsumer:
    def __init__(self, values: list[bytes]) -> None:
        self._m = [Message("k", v, f"0:{i}") for i, v in enumerate(values)]

    def poll(self, max_messages: int, timeout_s: float = 1.0) -> list[Message]:
        batch, self._m = self._m[:max_messages], self._m[max_messages:]
        return batch

    def commit(self) -> None: ...

    def close(self) -> None: ...


def ingest_simulation(lake: Lake) -> tuple[int, int]:
    """Ingest a small simulated stream into `lake`. Returns (valid, rejected) event counts."""
    result = simulate(SimulationConfig(days=2, customers=40, products=15, orders_per_day=25))
    stats = run_ingestion(
        ListConsumer([m.value for m in result.messages]),
        BronzeWriter(lake),
        batch_max_messages=2_000,
    )
    return stats.valid, stats.rejected


def dbt_build_on_s3(lake: Lake, endpoint_url: str, key: str, secret: str, workdir: Path) -> None:
    """Run `dbt build --target s3` against the lake. Raises AssertionError with dbt's output."""
    (workdir / "warehouse").mkdir(exist_ok=True)
    env = {
        **os.environ,
        "SHOPSTREAM_DATA_DIR": str(workdir),
        "SHOPSTREAM_LAKE_URI": lake.uri,
        "SHOPSTREAM_S3_ENDPOINT": endpoint_url.split("://", 1)[-1],
        "SHOPSTREAM_S3_ACCESS_KEY_ID": key,
        "SHOPSTREAM_S3_SECRET_ACCESS_KEY": secret,
        "SHOPSTREAM_S3_USE_SSL": "true" if endpoint_url.startswith("https") else "false",
        "SHOPSTREAM_S3_URL_STYLE": "path",
        "DBT_TARGET_PATH": str(workdir / "t"),
        "DBT_LOG_PATH": str(workdir / "l"),
    }
    proc = subprocess.run(
        [DBT, "build", "--target", "s3", "--project-dir", str(REPO / "dbt"),
         "--profiles-dir", str(REPO / "dbt")],
        env=env, capture_output=True, text=True, check=False,
    )  # fmt: skip
    assert proc.returncode == 0, proc.stdout + proc.stderr
