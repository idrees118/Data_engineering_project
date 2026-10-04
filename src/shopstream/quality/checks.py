"""Quality gates on the bronze layer. They run before any modelling so bad input fails fast.

Row-level and relational checks on modelled data live in dbt tests; these cover what dbt
cannot see: whether the *ingestion* itself looks healthy (volume, rejects, freshness).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


def _glob(root: Path, dataset: str) -> str:
    return str(root / dataset / "**" / "*.parquet")


def run_bronze_checks(
    bronze_dir: Path,
    *,
    max_dead_letter_ratio: float,
    max_freshness_hours: float | None,
    min_events: int = 1,
    now: datetime | None = None,
) -> list[CheckResult]:
    now = now or datetime.now(UTC)
    con = duckdb.connect(":memory:")
    results: list[CheckResult] = []
    try:
        events_glob = _glob(bronze_dir, "events")
        if not list(bronze_dir.glob("events/**/*.parquet")):
            return [CheckResult("bronze_not_empty", False, "no bronze event files found")]

        total, null_ids, distinct_ids, newest = con.execute(
            f"""select count(*), count(*) filter (where event_id is null),
                       count(distinct event_id), max(event_time)
                from read_parquet('{events_glob}', hive_partitioning=true)"""
        ).fetchone()  # type: ignore[misc]
        results.append(
            CheckResult("minimum_volume", total >= min_events, f"{total} events (min {min_events})")
        )
        results.append(CheckResult("no_null_event_ids", null_ids == 0, f"{null_ids} null ids"))
        dup_ratio = 1 - distinct_ids / total if total else 0.0
        results.append(
            CheckResult(
                "duplicates_reported",
                True,
                f"{total - distinct_ids} duplicate event_ids in bronze ({dup_ratio:.2%}); "
                "silver de-duplicates them",
            )
        )

        if max_freshness_hours is None:
            results.append(CheckResult("freshness", True, "skipped (no threshold configured)"))
        else:
            age_hours = (
                (now - newest.astimezone(UTC)).total_seconds() / 3600 if newest else float("inf")
            )
            results.append(
                CheckResult(
                    "freshness",
                    age_hours <= max_freshness_hours,
                    f"newest event is {age_hours:,.1f}h old (max {max_freshness_hours:g}h)",
                )
            )

        rejected = 0
        if list(bronze_dir.glob("dead_letter/**/*.parquet")):
            rejected = con.execute(
                f"select count(*) from read_parquet('{_glob(bronze_dir, 'dead_letter')}', "
                "hive_partitioning=true)"
            ).fetchone()[0]  # type: ignore[index]
        ratio = rejected / (total + rejected) if (total + rejected) else 0.0
        results.append(
            CheckResult(
                "dead_letter_ratio",
                ratio <= max_dead_letter_ratio,
                f"{rejected} rejected ({ratio:.2%}, max {max_dead_letter_ratio:.2%})",
            )
        )
    finally:
        con.close()
    return results
