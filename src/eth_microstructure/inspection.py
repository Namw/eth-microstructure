from decimal import Decimal
from pathlib import Path
from typing import Any

import orjson
import pyarrow.parquet as pq

from eth_microstructure.data_access import hourly_path, iso_utc


def _range(values: list[int]) -> dict[str, Any]:
    return {
        "first": values[0] if values else None,
        "first_iso": iso_utc(values[0]) if values else None,
        "last": values[-1] if values else None,
        "last_iso": iso_utc(values[-1]) if values else None,
    }


def inspect_file(
    data_dir: Path, stream: str, symbol: str, date: str, hour: int, limit: int
) -> dict[str, Any]:
    path = hourly_path(data_dir, stream, symbol, date, hour)
    if not path.exists():
        raise FileNotFoundError(path)
    table = pq.read_table(path)
    rows = table.slice(0, max(0, limit)).to_pylist()
    result: dict[str, Any] = {
        "dataset": stream,
        "path": str(path),
        "file_size": path.stat().st_size,
        "rows": table.num_rows,
        "schema": str(table.schema),
    }
    all_rows = table.to_pylist()
    if stream == "trades":
        event_times = [int(row["event_time"]) for row in all_rows]
        trade_times = [int(row["trade_time"]) for row in all_rows]
        ids = [int(row["aggregate_trade_id"]) for row in all_rows]
        result.update(
            event_time=_range(event_times),
            trade_time=_range(trade_times),
            aggregate_trade_id={"min": min(ids, default=None), "max": max(ids, default=None)},
        )
        for row in rows:
            row["event_time_iso"] = iso_utc(int(row["event_time"]))
            row["trade_time_iso"] = iso_utc(int(row["trade_time"]))
            row["aggressor_side"] = "SELL" if row["is_buyer_maker"] else "BUY"
            row["quote_amount"] = str(Decimal(row["price"]) * Decimal(row["quantity"]))
    else:
        event_column = "event_time" if "event_time" in table.column_names else "timestamp"
        event_times = [int(row[event_column]) for row in all_rows]
        ids = (
            [int(row["last_update_id"]) for row in all_rows if row["last_update_id"] is not None]
            if "last_update_id" in table.column_names
            else []
        )
        result.update(
            event_time=_range(event_times),
            last_update_id={"min": min(ids, default=None), "max": max(ids, default=None)},
            compatibility_warning=(
                None if ids else "legacy file has no last_update_id; ID range unavailable"
            ),
        )
        for row in rows:
            bids = orjson.loads(row["bids"])
            asks = orjson.loads(row["asks"])
            bid1 = Decimal(bids[0][0]) if bids else None
            ask1 = Decimal(asks[0][0]) if asks else None
            row["bids"] = bids
            row["asks"] = asks
            row["event_time_iso"] = iso_utc(int(row[event_column]))
            row["bid1"] = str(bid1) if bid1 is not None else None
            row["ask1"] = str(ask1) if ask1 is not None else None
            row["spread"] = str(ask1 - bid1) if bid1 is not None and ask1 is not None else None
            row["mid_price"] = (
                str((ask1 + bid1) / 2) if bid1 is not None and ask1 is not None else None
            )
            row["bid_depth_top20"] = str(sum(Decimal(level[1]) for level in bids[:20]))
            row["ask_depth_top20"] = str(sum(Decimal(level[1]) for level in asks[:20]))
    result["samples"] = rows
    return result
