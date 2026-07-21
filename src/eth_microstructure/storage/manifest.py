import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson
import pyarrow.compute as pc
import pyarrow.parquet as pq
from loguru import logger


class ManifestWriter:
    """Best-effort, rebuildable index written only after the data file is durable."""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.path = data_dir / "metadata" / "manifest.jsonl"

    def append(
        self,
        parquet_path: Path,
        dataset: str,
        symbol: str,
        event_column: str,
        id_column: str | None,
    ) -> None:
        try:
            table = pq.read_table(parquet_path)
            event_values = table[event_column]
            record: dict[str, Any] = {
                "dataset": dataset,
                "source": "binance",
                "market_type": "usd_m_futures",
                "symbol": symbol,
                "schema_version": 2 if dataset == "orderbook" else 1,
                "path": str(parquet_path.relative_to(self.data_dir)),
                "rows": table.num_rows,
                "min_event_time": int(pc.min(event_values).as_py()),
                "max_event_time": int(pc.max(event_values).as_py()),
                "file_size": parquet_path.stat().st_size,
                "sha256": self._sha256(parquet_path),
                "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            }
            if id_column and id_column in table.column_names:
                values = table[id_column]
                record[f"min_{id_column}"] = int(pc.min(values).as_py())
                record[f"max_{id_column}"] = int(pc.max(values).as_py())
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self.path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
            try:
                os.write(fd, orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE))
                os.fsync(fd)
            finally:
                os.close(fd)
        except Exception:
            logger.exception("Manifest update failed for {}; main data remains valid", parquet_path)

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
