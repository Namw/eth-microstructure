from datetime import UTC, datetime, timedelta
from pathlib import Path


def iso_utc(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, UTC).isoformat().replace("+00:00", "Z")


def parse_utc(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone, for example Z")
    return int(parsed.timestamp() * 1000)


def hourly_path(data_dir: Path, stream: str, symbol: str, date: str, hour: int) -> Path:
    return data_dir / stream / symbol.upper() / date / f"{hour:02d}.parquet"


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
