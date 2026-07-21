from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import orjson
import pyarrow.parquet as pq


def iso_utc(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, UTC).isoformat().replace("+00:00", "Z")


def parse_utc(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone, for example Z")
    return int(parsed.timestamp() * 1000)


def hourly_path(data_dir: Path, stream: str, symbol: str, date: str, hour: int) -> Path:
    return data_dir / stream / symbol.upper() / date / f"{hour:02d}.parquet"


def hourly_wal_path(data_dir: Path, stream: str, symbol: str, date: str, hour: int) -> Path:
    return data_dir / stream / symbol.upper() / date / f".{hour:02d}.wal"


def read_hour_rows(
    data_dir: Path, stream: str, symbol: str, date: str, hour: int
) -> tuple[list[dict[str, Any]], list[Path]]:
    """Read a completed hour and/or its active WAL without disturbing the writer."""
    parquet_path = hourly_path(data_dir, stream, symbol, date, hour)
    wal_path = hourly_wal_path(data_dir, stream, symbol, date, hour)
    rows: list[dict[str, Any]] = []
    sources: list[Path] = []
    if parquet_path.exists():
        rows.extend(pq.read_table(parquet_path).to_pylist())
        sources.append(parquet_path)
    if wal_path.exists():
        with wal_path.open("rb") as file:
            for line in file:
                if line.endswith(b"\n") and line.strip():
                    rows.append(orjson.loads(line))
        sources.append(wal_path)
    return rows, sources


def covered_files(
    data_dir: Path, stream: str, symbol: str, start_ms: int, end_ms: int
) -> list[Path]:
    moment = datetime.fromtimestamp(start_ms / 1000, UTC).replace(minute=0, second=0, microsecond=0)
    end = datetime.fromtimestamp((end_ms - 1) / 1000, UTC)
    paths: list[Path] = []
    while moment <= end:
        path = hourly_path(data_dir, stream, symbol, moment.strftime("%Y-%m-%d"), moment.hour)
        if path.exists():
            paths.append(path)
        moment += timedelta(hours=1)
    return paths
