from pathlib import Path

import pyarrow as pa
from conftest import write_rows

from eth_microstructure.cli import build_parser, cli_main
from eth_microstructure.inspection import inspect_file

TRADE_SCHEMA = pa.schema(
    [
        ("event_time", pa.int64()),
        ("trade_time", pa.int64()),
        ("price", pa.string()),
        ("quantity", pa.string()),
        ("is_buyer_maker", pa.bool_()),
        ("aggregate_trade_id", pa.int64()),
    ]
)


def test_inspection_derives_side_quote_and_iso(tmp_path: Path) -> None:
    path = tmp_path / "trades/ETHUSDT/2026-07-21/20.parquet"
    write_rows(
        path,
        [
            {
                "event_time": 1_784_664_000_000,
                "trade_time": 1_784_664_000_000,
                "price": "2.5",
                "quantity": "4",
                "is_buyer_maker": False,
                "aggregate_trade_id": 10,
            }
        ],
        TRADE_SCHEMA,
    )
    result = inspect_file(tmp_path, "trades", "ETHUSDT", "2026-07-21", 20, 20)
    assert result["samples"][0]["aggressor_side"] == "BUY"
    assert result["samples"][0]["quote_amount"] == "10.0"
    assert result["samples"][0]["event_time_iso"].endswith("Z")


def test_cli_inspect_and_parser_collect(tmp_path: Path, capsys: object) -> None:
    parser = build_parser()
    assert parser.parse_args(["collect"]).command == "collect"
    path = tmp_path / "trades/ETHUSDT/2026-07-21/20.parquet"
    write_rows(
        path,
        [
            {
                "event_time": 1_784_664_000_000,
                "trade_time": 1_784_664_000_000,
                "price": "1",
                "quantity": "1",
                "is_buyer_maker": True,
                "aggregate_trade_id": 1,
            }
        ],
        TRADE_SCHEMA,
    )
    assert cli_main(
        [
            "inspect", "trades", "--date", "2026-07-21", "--hour", "20",
            "--data-dir", str(tmp_path),
        ]
    ) == 0
