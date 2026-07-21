from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from eth_microstructure.storage import HourlyParquetWriter


def test_rotates_and_reads_parquet(tmp_path: Path) -> None:
    schema = pa.schema([("timestamp", pa.int64()), ("value", pa.string())])
    writer = HourlyParquetWriter(tmp_path, "ETHUSDT", schema, "timestamp", "timestamp")
    writer.append({"timestamp": 1_721_598_000_000, "value": "first"})
    writer.append({"timestamp": 1_721_601_600_000, "value": "second"})
    writer.close()

    files = sorted((tmp_path / "ETHUSDT").glob("*/*.parquet"))
    assert len(files) == 2
    assert sum(pq.read_table(file).num_rows for file in files) == 2


def test_recovery_deduplicates_after_atomic_write_crash(tmp_path: Path) -> None:
    schema = pa.schema([("timestamp", pa.int64()), ("value", pa.string())])
    writer = HourlyParquetWriter(tmp_path, "ETHUSDT", schema, "timestamp", "timestamp")
    record = {"timestamp": 1_721_598_000_000, "value": "same"}
    writer.append(record)
    parquet, wal = writer._paths(record["timestamp"])
    pq.write_table(pa.Table.from_pylist([record], schema=schema), parquet)
    writer.close()
    assert pq.read_table(parquet).num_rows == 1
    assert not wal.exists()
