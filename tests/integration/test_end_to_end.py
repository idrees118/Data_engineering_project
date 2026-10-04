"""Full pipeline: simulate -> stream -> bronze -> dbt (silver/gold) -> compare with ground truth.

The expected numbers are recomputed here in plain Python straight from the simulator's *clean*
events, independently of the SQL under test. If duplicates, late arrivals or malformed messages
leaked through any layer, or an incremental load lost data, these assertions fail.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections import defaultdict
from decimal import Decimal
from itertools import pairwise
from pathlib import Path

import duckdb
import pytest

from shopstream.events import EventType
from shopstream.generator import SimulationConfig, SimulationResult, simulate
from shopstream.ingestion import BronzeWriter, run_ingestion
from shopstream.streaming.file_transport import FileConsumer, FilePublisher

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
DBT = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
CONFIG = SimulationConfig(days=6, customers=120, products=30, orders_per_day=60, seed=11)

pytest.importorskip("dbt.cli.main", reason="dbt-duckdb is not installed")


def _dbt(data_dir: Path, *args: str) -> None:
    env = {
        **os.environ,
        "SHOPSTREAM_DATA_DIR": str(data_dir),
        "DBT_TARGET_PATH": str(data_dir / "dbt_target"),
        "DBT_LOG_PATH": str(data_dir / "dbt_logs"),
    }
    proc = subprocess.run(
        [DBT, *args, "--project-dir", str(REPO / "dbt"), "--profiles-dir", str(REPO / "dbt")],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"dbt {' '.join(args)} failed:\n{proc.stdout}\n{proc.stderr}"


def _publish(data_dir: Path, result: SimulationResult, lo: int, hi: int) -> None:
    publisher = FilePublisher(data_dir / "landing")
    for message in result.messages[lo:hi]:
        publisher.publish(message.key, message.value)
    publisher.close()


def _ingest(data_dir: Path) -> None:
    consumer = FileConsumer(data_dir / "landing", data_dir / "cp.json")
    # dedup_window=1 effectively disables the in-memory filter, so duplicates reach bronze and
    # silver's own de-duplication is what is actually being tested.
    run_ingestion(
        consumer,
        BronzeWriter(data_dir / "lake" / "bronze"),
        batch_max_messages=3_000,
        dedup_window=1,
    )


def _expected(result: SimulationResult) -> dict[str, object]:
    orders: dict[str, dict] = {}  # type: ignore[type-arg]
    for e in result.iter_clean(EventType.ORDER_PLACED):
        p = e.payload
        gross = sum(Decimal(str(i["quantity"])) * Decimal(str(i["unit_price"])) for i in p["items"])  # type: ignore[attr-defined,union-attr]
        orders[p["order_id"]] = {
            "net": gross - Decimal(str(p["discount_amount"])),  # type: ignore[arg-type]
            "paid": False,
            "refunded": False,
            "date": e.event_time.date(),
        }
    for e in result.iter_clean(EventType.PAYMENT_PROCESSED):
        if e.payload["status"] == "succeeded":
            orders[e.payload["order_id"]]["paid"] = True
    for e in result.iter_clean(EventType.ORDER_STATUS_CHANGED):
        if e.payload["status"] == "refunded":
            orders[e.payload["order_id"]]["refunded"] = True

    revenue_by_day: dict[object, Decimal] = defaultdict(Decimal)
    for o in orders.values():
        if o["paid"] and not o["refunded"]:
            revenue_by_day[o["date"]] += o["net"]
    return {
        "orders": len(orders),
        "revenue_by_day": dict(revenue_by_day),
        "net_revenue": sum(revenue_by_day.values(), Decimal(0)),
    }


def _assert_warehouse_matches(data_dir: Path, result: SimulationResult) -> None:
    expected = _expected(result)
    con = duckdb.connect(str(data_dir / "warehouse" / "shopstream.duckdb"), read_only=True)
    try:
        assert con.sql("select count(*) from staging.stg_events").fetchone()[0] == len(  # type: ignore[index]
            result.clean_events
        ), "silver must hold exactly the clean events, once each"
        assert con.sql("select count(*) from marts.fct_orders").fetchone()[0] == expected["orders"]  # type: ignore[index]
        total = con.sql("select sum(net_revenue) from marts.fct_orders").fetchone()[0]  # type: ignore[index]
        assert Decimal(total) == expected["net_revenue"]

        channels = dict(
            con.sql("select channel, count(*) from marts.fct_orders group by 1").fetchall()
        )
        expected_channels: dict[str, int] = defaultdict(int)
        for e in result.iter_clean(EventType.ORDER_PLACED):
            expected_channels[str(e.payload.get("channel", "unknown"))] += 1
        assert channels == dict(expected_channels), "mixed v1/v2 orders must be counted exactly"
        assert "unknown" in channels and len(channels) > 1, "test must cover both schema versions"

        daily = dict(
            con.sql("select order_date, net_revenue from marts.mart_daily_revenue").fetchall()
        )
        for day, value in expected["revenue_by_day"].items():  # type: ignore[attr-defined]
            assert Decimal(daily[day]) == value, f"revenue mismatch on {day}"
    finally:
        con.close()


@pytest.fixture(scope="module")
def simulation() -> SimulationResult:
    return simulate(CONFIG)


def test_full_load_matches_ground_truth(tmp_path: Path, simulation: SimulationResult) -> None:
    (tmp_path / "warehouse").mkdir()
    _publish(tmp_path, simulation, 0, len(simulation.messages))
    _ingest(tmp_path)
    _dbt(tmp_path, "build")
    _assert_warehouse_matches(tmp_path, simulation)


def test_incremental_loads_converge_to_the_same_result(
    tmp_path: Path, simulation: SimulationResult
) -> None:
    """Two loads, each followed by `dbt build`, must equal one full load.

    The split point lands mid-stream, so orders are loaded before their payments / status
    changes, and duplicates and late events straddle the two loads.
    """
    (tmp_path / "warehouse").mkdir()
    cut = len(simulation.messages) // 2
    _publish(tmp_path, simulation, 0, cut)
    _ingest(tmp_path)
    _dbt(tmp_path, "build")

    _publish(tmp_path, simulation, cut, len(simulation.messages))
    _ingest(tmp_path)
    _dbt(tmp_path, "build")
    _assert_warehouse_matches(tmp_path, simulation)

    _dbt(tmp_path, "build")  # a third run with no new data must change nothing
    _assert_warehouse_matches(tmp_path, simulation)


def test_scd2_has_one_current_row_per_customer_and_tracks_changes(
    tmp_path: Path, simulation: SimulationResult
) -> None:
    (tmp_path / "warehouse").mkdir()
    _publish(tmp_path, simulation, 0, len(simulation.messages))
    _ingest(tmp_path)
    _dbt(tmp_path, "run", "--select", "+dim_customers")

    attribute_changes: dict[str, set[tuple[object, object]]] = defaultdict(set)
    for e in simulation.iter_clean(EventType.CUSTOMER_UPDATED):
        attribute_changes[e.payload["customer_id"]].add(  # type: ignore[index]
            (e.event_time, (e.payload["country"], e.payload["tier"]))
        )

    def versions(history: set[tuple[object, object]]) -> int:
        ordered = sorted(history, key=lambda t: t[0])  # type: ignore[arg-type,return-value]
        return 1 + sum(1 for a, b in pairwise(ordered) if a[1] != b[1])

    expected_rows = sum(versions(h) for h in attribute_changes.values())
    con = duckdb.connect(str(tmp_path / "warehouse" / "shopstream.duckdb"), read_only=True)
    try:
        assert con.sql("select count(*) from marts.dim_customers").fetchone()[0] == expected_rows  # type: ignore[index]
        assert con.sql("select count(*) from marts.dim_customers where is_current").fetchone()[
            0
        ] == len(attribute_changes)  # type: ignore[index]
    finally:
        con.close()
