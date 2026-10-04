"""Pipeline metrics in the Prometheus text exposition format.

Written as a *textfile* (node-exporter `--collector.textfile.directory`) instead of running an
HTTP server: the ingestion job is a short-lived batch process, so there is nothing to scrape
between runs. The file is replaced atomically so a scraper never reads a half-written one.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

PREFIX = "shopstream_ingest"

_HELP: dict[str, tuple[str, str]] = {
    "messages_received_total": ("counter", "Messages read from the stream in the last run."),
    "events_valid_total": ("counter", "Messages that passed contract validation."),
    "events_rejected_total": ("counter", "Messages routed to the dead-letter dataset."),
    "duplicates_dropped_total": ("counter", "Duplicate events dropped before writing bronze."),
    "batches_total": ("counter", "Micro-batches written in the last run."),
    "dead_letter_ratio": ("gauge", "Rejected messages divided by received messages."),
    "run_duration_seconds": ("gauge", "Wall-clock duration of the last run."),
    "last_run_timestamp_seconds": ("gauge", "Unix time at which the last run finished."),
}


def render(values: Mapping[str, float]) -> str:
    """Render known metrics; unknown names are rejected so typos fail loudly."""
    unknown = set(values) - set(_HELP)
    if unknown:
        raise KeyError(f"unknown metrics: {sorted(unknown)}")
    lines: list[str] = []
    for name, value in values.items():
        kind, help_text = _HELP[name]
        full = f"{PREFIX}_{name}"
        lines += [f"# HELP {full} {help_text}", f"# TYPE {full} {kind}", f"{full} {value:g}"]
    return "\n".join(lines) + "\n"


def write_textfile(path: Path, values: Mapping[str, float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(render(values))
    os.replace(tmp, path)
