"""Command line entry point: `shopstream <generate|ingest|quality|report>`."""

from __future__ import annotations

import argparse
import sys
from datetime import date

import duckdb

from shopstream.config import get_settings
from shopstream.generator import SimulationConfig, simulate
from shopstream.ingestion import BronzeWriter, run_ingestion
from shopstream.logging_setup import configure_logging
from shopstream.quality import run_bronze_checks
from shopstream.streaming import build_consumer, build_publisher


def _cmd_generate(args: argparse.Namespace) -> int:
    cfg = SimulationConfig(
        seed=args.seed,
        start_date=date.fromisoformat(args.start_date),
        days=args.days,
        orders_per_day=args.orders_per_day,
        customers=args.customers,
    )
    result = simulate(cfg)
    publisher = build_publisher(get_settings())
    for message in result.messages:
        publisher.publish(message.key, message.value)
    publisher.flush()
    print(f"published {len(result.messages):,} messages  {result.stats}")
    return 0


def _cmd_ingest(args: argparse.Namespace) -> int:
    settings = get_settings()
    consumer = build_consumer(settings)
    try:
        stats = run_ingestion(
            consumer,
            BronzeWriter(settings.lake()),
            batch_max_messages=settings.batch_max_messages,
            dedup_window=settings.dedup_window,
            idle_timeout_s=args.follow,
            run_log=settings.data_dir / "lake" / "_meta" / "ingest_runs.jsonl",
            metrics_path=settings.data_dir / "lake" / "_meta" / "ingest.prom",
        )
    finally:
        consumer.close()
    print(f"ingested {stats}")
    return 0


def _cmd_quality(_: argparse.Namespace) -> int:
    settings = get_settings()
    results = run_bronze_checks(
        settings.lake(),
        max_dead_letter_ratio=settings.max_dead_letter_ratio,
        max_freshness_hours=settings.max_freshness_hours,
    )
    for r in results:
        print(f"[{'PASS' if r.passed else 'FAIL'}] {r.name}: {r.detail}")
    return 0 if all(r.passed for r in results) else 1


def _cmd_report(_: argparse.Namespace) -> int:
    settings = get_settings()
    if not settings.warehouse_path.exists():
        print("warehouse not built yet: run `make dbt-build` first", file=sys.stderr)
        return 1
    con = duckdb.connect(str(settings.warehouse_path), read_only=True)
    queries = {
        "Daily revenue (last 7 days)": """
            select order_date, orders, paid_orders, net_revenue, refunds, aov
            from marts.mart_daily_revenue order by order_date desc limit 7""",
        "Revenue by category": """
            select category, sum(units_sold) as units, round(sum(net_revenue), 2) as net_revenue
            from marts.mart_category_performance group by 1 order by net_revenue desc""",
        "Customer segments": """
            select segment, count(*) as customers, round(sum(lifetime_net_revenue), 0) as revenue
            from marts.mart_customer_summary group by 1 order by revenue desc""",
        "Conversion funnel (last 5 days)": """
            select session_date, sessions, product_sessions, checkout_sessions, orders,
                   round(session_to_order_rate * 100, 2) as conv_pct
            from marts.mart_conversion_funnel order by session_date desc limit 5""",
    }
    for title, sql in queries.items():
        print(f"\n== {title}")
        cursor = con.execute(sql)
        header = [d[0] for d in cursor.description]
        print(_format_table(header, cursor.fetchall()))
    return 0


def _format_table(header: list[str], rows: list[tuple[object, ...]]) -> str:
    cells = [header, *[[_fmt(v) for v in row] for row in rows]]
    widths = [max(len(r[i]) for r in cells) for i in range(len(header))]
    lines = ["  ".join(c.rjust(w) for c, w in zip(r, widths, strict=True)) for r in cells]
    return "\n".join(lines)


def _fmt(value: object) -> str:
    if isinstance(value, float):
        return f"{value:,.2f}"
    return "" if value is None else str(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="shopstream", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="simulate shop traffic and publish it to the stream")
    gen.add_argument("--seed", type=int, default=42)
    gen.add_argument("--start-date", default="2025-01-01")
    gen.add_argument("--days", type=int, default=14)
    gen.add_argument("--orders-per-day", type=int, default=150)
    gen.add_argument("--customers", type=int, default=500)
    gen.set_defaults(func=_cmd_generate)

    ing = sub.add_parser("ingest", help="drain the stream into the bronze layer")
    ing.add_argument(
        "--follow",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="keep waiting for new data until idle this long (default: drain & exit)",
    )
    ing.set_defaults(func=_cmd_ingest)

    sub.add_parser("quality", help="run bronze quality gates").set_defaults(func=_cmd_quality)
    sub.add_parser("report", help="print headline numbers from the gold marts").set_defaults(
        func=_cmd_report
    )

    args = parser.parse_args(argv)
    configure_logging()
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
