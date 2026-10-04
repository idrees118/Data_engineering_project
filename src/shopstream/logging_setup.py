"""Structured (key=value) logging so pipeline runs are greppable and machine-parseable."""

from __future__ import annotations

import logging
import sys
from typing import Any


class _KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = (
            f"{self.formatTime(record, '%Y-%m-%dT%H:%M:%S')} level={record.levelname} "
            f"logger={record.name} msg={record.getMessage()!r}"
        )
        extras: dict[str, Any] = getattr(record, "ctx", {})
        pairs = " ".join(f"{k}={v}" for k, v in extras.items())
        return f"{base} {pairs}".rstrip()


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_KeyValueFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)


def log_kv(logger: logging.Logger, msg: str, **ctx: Any) -> None:
    logger.info(msg, extra={"ctx": ctx})
