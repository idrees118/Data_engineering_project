"""Runtime configuration, loaded from SHOPSTREAM_* environment variables."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shopstream.storage import Lake


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SHOPSTREAM_", env_file=".env", extra="ignore")

    data_dir: Path = Field(default=Path("data"), description="Root for all local pipeline state.")
    transport: Literal["file", "kafka"] = "file"

    # Lake location. Unset = local directory under data_dir. Set to e.g. s3://bucket/bronze to use
    # S3 or MinIO; for MinIO also set s3_endpoint_url (e.g. http://localhost:9000).
    lake_uri: str | None = None
    s3_endpoint_url: str | None = None
    s3_access_key_id: str | None = None
    s3_secret_access_key: str | None = None
    s3_region: str = "us-east-1"

    # Kafka-compatible broker (Redpanda in docker-compose).
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_topic: str = "shopstream.events"
    kafka_consumer_group: str = "shopstream-ingest"

    # Micro-batch behaviour of the ingestion job.
    batch_max_messages: int = Field(default=5_000, gt=0)
    dedup_window: int = Field(default=200_000, gt=0, description="Recent event ids kept in memory.")

    # Bronze compaction: only partitions untouched for this long are merged.
    compaction_min_age_seconds: float = Field(default=3600.0, ge=0)

    # Data-quality gates (see shopstream.quality).
    max_dead_letter_ratio: float = Field(default=0.05, ge=0, le=1)
    max_freshness_hours: float | None = Field(
        default=None,
        gt=0,
        description="Fail if the newest event is older than this. Off by default.",
    )

    @field_validator(
        "lake_uri", "s3_endpoint_url", "s3_access_key_id", "s3_secret_access_key", mode="before"
    )
    @classmethod
    def _blank_is_unset(cls, value: object) -> object:
        return None if isinstance(value, str) and not value.strip() else value

    @property
    def landing_dir(self) -> Path:
        return self.data_dir / "landing"

    @property
    def checkpoint_path(self) -> Path:
        return self.data_dir / "checkpoints" / "ingest.json"

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "lake" / "bronze"

    def lake(self) -> Lake:
        return Lake.from_settings(self)

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse" / "shopstream.duckdb"


@lru_cache
def get_settings() -> Settings:
    return Settings()
