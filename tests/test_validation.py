from pathlib import Path

import orjson
import pyarrow as pa
from conftest import write_rows

from eth_microstructure.validation import validate_orderbook_file, validate_trade_file

TRADE_SCHEMA = pa.schema(
    [
        ("event_time", pa.int64()), ("trade_time", pa.int64()), ("price", pa.string()),
        ("quantity", pa.string()), ("is_buyer_maker", pa.bool_()),
        ("aggregate_trade_id", pa.int64()),
    ]
)
BOOK_SCHEMA = pa.schema(
    [
        ("timestamp", pa.int64()), ("event_time", pa.int64()),
        ("last_update_id", pa.int64()), ("bids", pa.string()), ("asks", pa.string()),
    ]
)
START = 1_784_664_000_000


def _trade(identifier: int, offset: int, price: str = "1") -> dict[str, object]:
    return {
        "event_time": START + offset, "trade_time": START + offset, "price": price,
        "quantity": "1", "is_buyer_maker": False, "aggregate_trade_id": identifier,
    }


def test_trade_validation_detects_gap_duplicate_reverse_and_bad_value(tmp_path: Path) -> None:
    path = tmp_path / "2026-07-21/20.parquet"
    rows = [_trade(1, 0), _trade(3, 1000), _trade(3, 2000), _trade(2, 3000, "0")]
    report = validate_trade_file(write_rows(path, rows, TRADE_SCHEMA))
    assert report["status"] == "FAIL"
    assert report["metrics"]["missing_ids"] == 1
    assert report["metrics"]["duplicate_ids"] == 1
    assert report["metrics"]["reversed_ids"] == 1


def _book(
    second: int, update_id: int, bids: list[list[str]], asks: list[list[str]]
) -> dict[str, object]:
    timestamp = START + second * 1000
    return {
        "timestamp": timestamp, "event_time": timestamp, "last_update_id": update_id,
        "bids": orjson.dumps(bids).decode(), "asks": orjson.dumps(asks).decode(),
    }


def test_orderbook_validation_detects_duplicate_reverse_sort_and_cross(tmp_path: Path) -> None:
    path = tmp_path / "2026-07-21/20.parquet"
    rows = [
        _book(0, 2, [["10", "1"], ["11", "1"]], [["9", "1"], ["8", "1"]]),
        _book(0, 1, [["10", "1"]], [["11", "1"]]),
    ]
    report = validate_orderbook_file(write_rows(path, rows, BOOK_SCHEMA))
    assert report["status"] == "FAIL"
    assert report["metrics"]["duplicate_seconds"] == 1
    assert report["metrics"]["last_update_id_reversals"] == 1
    assert any("unsorted bids" in error for error in report["errors"])
    assert any("crossed" in error for error in report["errors"])
    assert report["metrics"]["longest_missing_seconds"] > 0
