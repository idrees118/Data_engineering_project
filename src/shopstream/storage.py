"""Where the lake lives: a local directory or an S3-compatible bucket (AWS S3, MinIO).

Everything that touches lake files (ingestion, compaction, quality checks) goes through `Lake`, so
the storage location is one setting (`SHOPSTREAM_LAKE_URI`) and not a code change. Writes and
listing use `pyarrow.fs`; DuckDB reads the same files through its own `httpfs` extension, which
`configure_duckdb` sets up.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import duckdb
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from shopstream.config import Settings


@dataclass(frozen=True)
class S3Options:
    endpoint_url: str | None = None  # e.g. http://localhost:9000 for MinIO; None = real AWS
    access_key_id: str | None = None
    secret_access_key: str | None = None
    region: str = "us-east-1"


class Lake:
    def __init__(self, uri: str, s3: S3Options | None = None) -> None:
        self.uri = uri.rstrip("/")
        parsed = urlparse(self.uri)
        self.is_s3 = parsed.scheme == "s3"
        self.s3 = s3 or S3Options()
        if self.is_s3:
            self._base = f"{parsed.netloc}{parsed.path}".rstrip("/")
            self.fs: pafs.FileSystem = self._s3_filesystem()
        else:
            self._base = str(Path(self.uri).resolve())
            self.fs = pafs.LocalFileSystem()

    @classmethod
    def local(cls, path: Path) -> Lake:
        return cls(str(path))

    @classmethod
    def from_settings(cls, settings: Settings) -> Lake:
        if settings.lake_uri:
            return cls(
                settings.lake_uri,
                S3Options(
                    settings.s3_endpoint_url,
                    settings.s3_access_key_id,
                    settings.s3_secret_access_key,
                    settings.s3_region,
                ),
            )
        return cls.local(settings.bronze_dir)

    def _s3_filesystem(self) -> pafs.S3FileSystem:
        kwargs: dict[str, object] = {"region": self.s3.region}
        if self.s3.access_key_id and self.s3.secret_access_key:
            kwargs["access_key"] = self.s3.access_key_id
            kwargs["secret_key"] = self.s3.secret_access_key
        if self.s3.endpoint_url:
            endpoint = urlparse(self.s3.endpoint_url)
            kwargs["endpoint_override"] = endpoint.netloc
            kwargs["scheme"] = endpoint.scheme or "https"
        return pafs.S3FileSystem(**kwargs)

    # -- paths ---------------------------------------------------------------------------------
    def path(self, relative: str) -> str:
        """Path in the form `pyarrow.fs` expects (no scheme for S3)."""
        return f"{self._base}/{relative}".rstrip("/")

    def duckdb_uri(self, relative: str) -> str:
        return f"s3://{self.path(relative)}" if self.is_s3 else self.path(relative)

    def relative(self, fs_path: str) -> str:
        return fs_path[len(self._base) + 1 :]

    # -- files ---------------------------------------------------------------------------------
    def files(self, prefix: str) -> list[str]:
        """All finished parquet files under `prefix`, as fs paths. Hidden temp files are skipped."""
        selector = pafs.FileSelector(self.path(prefix), recursive=True, allow_not_found=True)
        found = [
            info.path
            for info in self.fs.get_file_info(selector)
            if info.type == pafs.FileType.File
            and info.path.endswith(".parquet")
            and not info.base_name.startswith(".")
        ]
        return sorted(found)

    def has_files(self, prefix: str) -> bool:
        return bool(self.files(prefix))

    def write_parquet(self, table: pa.Table, relative: str) -> str:
        """Write one file so readers never see a partial one.

        Local: write a hidden temp file then rename (atomic on POSIX). S3: a single PUT is already
        all-or-nothing, so the object is written directly.
        """
        target = self.path(relative)
        if self.is_s3:
            pq.write_table(table, target, filesystem=self.fs, compression="zstd")
            return target
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        tmp = str(Path(target).with_name(f".{Path(target).name}.tmp"))
        pq.write_table(table, tmp, compression="zstd")
        os.replace(tmp, target)
        return target

    def read_parquet(self, fs_path: str) -> pa.Table:
        return pq.read_table(fs_path, filesystem=self.fs)

    def delete(self, fs_paths: list[str]) -> None:
        for fs_path in fs_paths:
            self.fs.delete_file(fs_path)

    def modified_at(self, fs_path: str) -> float:
        mtime = self.fs.get_file_info(fs_path).mtime
        return mtime.timestamp() if mtime else 0.0

    # -- DuckDB --------------------------------------------------------------------------------
    def configure_duckdb(self, con: duckdb.DuckDBPyConnection) -> None:
        """Make a DuckDB connection able to read this lake. A no-op for local directories."""
        if not self.is_s3:
            return
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute(f"SET s3_region='{self.s3.region}'")
        if self.s3.access_key_id and self.s3.secret_access_key:
            con.execute(f"SET s3_access_key_id='{self.s3.access_key_id}'")
            con.execute(f"SET s3_secret_access_key='{self.s3.secret_access_key}'")
        if self.s3.endpoint_url:
            endpoint = urlparse(self.s3.endpoint_url)
            con.execute(f"SET s3_endpoint='{endpoint.netloc}'")
            con.execute(f"SET s3_use_ssl={'true' if endpoint.scheme == 'https' else 'false'}")
            con.execute("SET s3_url_style='path'")  # MinIO / S3-compatible stores need path style
