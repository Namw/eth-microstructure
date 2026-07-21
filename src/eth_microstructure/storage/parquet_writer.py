import os
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from loguru import logger

from eth_microstructure.storage.manifest import ManifestWriter


class HourlyParquetWriter:
    """Durable WAL-backed hourly Parquet storage.

    Each record is fsynced to a newline-delimited WAL before acknowledgment. Completed
    hours are converted to Parquet through a temporary file and atomic rename.
    """

    def __init__(
        self,
        root: Path,
        symbol: str,
        schema: pa.Schema,
        timestamp_column: str,
        unique_column: str,
        fsync_every: int = 1,
        event_column: str | None = None,
        manifest: ManifestWriter | None = None,
    ) -> None:
        self.root = root / symbol.upper()
        self.schema = schema
        self.timestamp_column = timestamp_column
        self.unique_column = unique_column
        self.fsync_every = fsync_every
        self.dataset = root.name
        self.symbol = symbol.upper()
        self.event_column = event_column or timestamp_column
        self.manifest = manifest
        self._handles: dict[Path, Any] = {}
        self._unflushed: dict[Path, int] = {}

    def _paths(self, timestamp_ms: int) -> tuple[Path, Path]:
        moment = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
        directory = self.root / moment.strftime("%Y-%m-%d")
        parquet = directory / f"{moment:%H}.parquet"
        return parquet, directory / f".{moment:%H}.wal"

    def append(self, record: Mapping[str, Any]) -> None:
        timestamp = int(record[self.timestamp_column])
        parquet_path, wal_path = self._paths(timestamp)
        wal_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self._handles.get(wal_path)
        if handle is None:
            handle = wal_path.open("ab", buffering=0)
            self._handles[wal_path] = handle
            self._unflushed[wal_path] = 0
        handle.write(orjson.dumps(dict(record), option=orjson.OPT_APPEND_NEWLINE))
        self._unflushed[wal_path] += 1
        if self._unflushed[wal_path] >= self.fsync_every:
            os.fsync(handle.fileno())
            self._unflushed[wal_path] = 0
        self._finalize_before(parquet_path)

    def _finalize_before(self, current_path: Path) -> None:
        for wal_path in sorted(self.root.glob("*/.*.wal")):
            parquet_path = wal_path.with_name(f"{wal_path.stem[1:]}.parquet")
            if parquet_path < current_path:
                self._finalize(wal_path, parquet_path)

    def _finalize(self, wal_path: Path, parquet_path: Path) -> None:
        handle = self._handles.pop(wal_path, None)
        if handle is not None:
            os.fsync(handle.fileno())
            handle.close()
        self._unflushed.pop(wal_path, None)
        if not wal_path.exists():
            return

        rows: list[dict[str, Any]] = []
        if parquet_path.exists():
            rows.extend(pq.read_table(parquet_path).to_pylist())
        with wal_path.open("rb") as file:
            for line in file:
                if line.strip():
                    rows.append(orjson.loads(line))
        if not rows:
            wal_path.unlink(missing_ok=True)
            return

        deduplicated = {row[self.unique_column]: row for row in rows}
        ordered = sorted(deduplicated.values(), key=lambda row: row[self.unique_column])
        table = pa.Table.from_pylist(ordered, schema=self.schema)
        temporary = parquet_path.with_suffix(".parquet.tmp")
        pq.write_table(table, temporary, compression="zstd")
        with temporary.open("rb") as file:
            os.fsync(file.fileno())
        os.replace(temporary, parquet_path)
        directory_fd = os.open(parquet_path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        wal_path.unlink()
        logger.info("Finalized {} rows to {}", len(ordered), parquet_path)
        if self.manifest is not None:
            id_column = (
                "aggregate_trade_id" if self.dataset == "trades" else "last_update_id"
            )
            self.manifest.append(
                parquet_path, self.dataset, self.symbol, self.event_column, id_column
            )

    def recover(self) -> None:
        """Finalize WAL files from prior hours left by a crash."""
        now_path, _ = self._paths(int(datetime.now(UTC).timestamp() * 1000))
        self._finalize_before(now_path)

    def max_value(self) -> int | None:
        maximum: int | None = None
        for parquet_path in self.root.glob("*/*.parquet"):
            values = pq.read_table(parquet_path, columns=[self.unique_column]).column(0)
            if len(values):
                value = int(pc.max(values).as_py())
                maximum = value if maximum is None else max(maximum, value)
        for wal_path in self.root.glob("*/.*.wal"):
            with wal_path.open("rb") as file:
                for line in file:
                    if line.strip():
                        value = int(orjson.loads(line)[self.unique_column])
                        maximum = value if maximum is None else max(maximum, value)
        return maximum

    def close(self) -> None:
        for wal_path in list(self._handles):
            parquet_path = wal_path.with_name(f"{wal_path.stem[1:]}.parquet")
            self._finalize(wal_path, parquet_path)

    def wal_size(self) -> int:
        return sum(path.stat().st_size for path in self.root.glob("*/.*.wal"))

    def current_output_hour(self) -> str:
        moment = datetime.now(UTC)
        return moment.strftime("%Y-%m-%dT%H:00:00Z")
