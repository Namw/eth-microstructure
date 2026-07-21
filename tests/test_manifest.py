import json
from pathlib import Path

import pyarrow as pa

from eth_microstructure.storage import HourlyParquetWriter, ManifestWriter


def test_manifest_written_after_parquet(tmp_path: Path) -> None:
    schema = pa.schema([("event_time", pa.int64()), ("aggregate_trade_id", pa.int64())])
    manifest = ManifestWriter(tmp_path)
    writer = HourlyParquetWriter(
        tmp_path / "trades", "ETHUSDT", schema, "event_time", "aggregate_trade_id",
        event_column="event_time", manifest=manifest,
    )
    writer.append({"event_time": 1_784_664_000_000, "aggregate_trade_id": 10})
    writer.close()
    record = json.loads((tmp_path / "metadata/manifest.jsonl").read_text().strip())
    assert record["dataset"] == "trades"
    assert record["rows"] == 1
    assert record["min_aggregate_trade_id"] == 10
    assert len(record["sha256"]) == 64
