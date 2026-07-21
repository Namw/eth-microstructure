from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from itertools import pairwise
from pathlib import Path
from typing import Any

import orjson
import pyarrow.parquet as pq


def _status(errors: list[str], warnings: list[str]) -> str:
    if errors:
        return "FAIL"
    return "PASS_WITH_WARNINGS" if warnings else "PASS"


def _parse_positive(value: Any, allow_zero: bool = False) -> bool:
    try:
        number = Decimal(str(value))
        return number >= 0 if allow_zero else number > 0
    except (InvalidOperation, ValueError):
        return False


def _longest_gap(times: list[int]) -> int:
    return max((right - left for left, right in pairwise(times)), default=0)


def validate_trade_file(path: Path) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    try:
        rows = pq.read_table(path).to_pylist()
    except Exception as exc:
        return {"path": str(path), "status": "FAIL", "errors": [f"unreadable: {exc}"]}
    ids = [int(row["aggregate_trade_id"]) for row in rows]
    events = [int(row["event_time"]) for row in rows]
    trades = [int(row["trade_time"]) for row in rows]
    duplicate_ids = len(ids) - len(set(ids))
    reversed_ids = sum(right < left for left, right in pairwise(ids))
    missing_ids = sum(max(0, right - left - 1) for left, right in pairwise(ids))
    if duplicate_ids:
        errors.append(f"{duplicate_ids} duplicate aggregate_trade_id values")
    if reversed_ids:
        errors.append(f"{reversed_ids} aggregate_trade_id reversals")
    if missing_ids:
        errors.append(f"{missing_ids} aggregate_trade_id values missing")
    event_reversals = [left - right for left, right in pairwise(events) if right < left]
    trade_reversals = [left - right for left, right in pairwise(trades) if right < left]
    if any(value > 1000 for value in event_reversals + trade_reversals):
        errors.append("event_time or trade_time reverses by more than 1000ms")
    elif event_reversals or trade_reversals:
        warnings.append("minor event_time or trade_time reversal")
    invalid_values = sum(
        not _parse_positive(row["price"]) or not _parse_positive(row["quantity"]) for row in rows
    )
    if invalid_values:
        errors.append(f"{invalid_values} non-positive or invalid price/quantity records")
    expected = datetime.strptime(
        f"{path.parent.name} {path.stem}", "%Y-%m-%d %H"
    ).replace(tzinfo=UTC)
    start_ms = int(expected.timestamp() * 1000)
    outside = sum(not start_ms <= value < start_ms + 3_600_000 for value in trades)
    if outside:
        errors.append(f"{outside} records outside the UTC file hour")
    per_minute = Counter(
        datetime.fromtimestamp(value / 1000, UTC).strftime("%H:%M") for value in trades
    )
    report: dict[str, Any] = {
        "path": str(path),
        "rows": len(rows),
        "metrics": {
            "duplicate_ids": duplicate_ids,
            "reversed_ids": reversed_ids,
            "missing_ids": missing_ids,
            "longest_no_trade_ms": _longest_gap(trades),
            "records_per_minute": dict(sorted(per_minute.items())),
        },
        "errors": errors,
        "warnings": warnings,
    }
    report["status"] = _status(errors, warnings)
    return report


def validate_orderbook_file(path: Path) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    try:
        table = pq.read_table(path)
        rows = table.to_pylist()
    except Exception as exc:
        return {"path": str(path), "status": "FAIL", "errors": [f"unreadable: {exc}"]}
    event_column = "event_time" if "event_time" in table.column_names else "timestamp"
    times = [int(row[event_column]) for row in rows]
    seconds = [value // 1000 for value in times]
    duplicates = sum(count - 1 for count in Counter(seconds).values() if count > 1)
    expected = datetime.strptime(
        f"{path.parent.name} {path.stem}", "%Y-%m-%d %H"
    ).replace(tzinfo=UTC)
    start_second = int(expected.timestamp())
    expected_seconds = set(range(start_second, start_second + 3600))
    missing = sorted(expected_seconds - set(seconds))
    longest_missing = 0
    current = 0
    previous: int | None = None
    for second in missing:
        current = current + 1 if previous is not None and second == previous + 1 else 1
        longest_missing = max(longest_missing, current)
        previous = second
    if duplicates:
        errors.append(f"{duplicates} duplicate snapshot seconds")
    if missing:
        warnings.append(f"{len(missing)} missing snapshot seconds")
    if "last_update_id" not in table.column_names:
        warnings.append("legacy schema: last_update_id unavailable")
        update_reversals = None
    else:
        ids = [int(row["last_update_id"]) for row in rows if row["last_update_id"] is not None]
        if len(ids) != len(rows):
            warnings.append("some legacy rows have no last_update_id")
        update_reversals = sum(right < left for left, right in pairwise(ids))
        if update_reversals:
            errors.append(f"{update_reversals} last_update_id reversals")
    invalid_books = 0
    crossed_books = 0
    bid_order_errors = 0
    ask_order_errors = 0
    for row in rows:
        try:
            bids = orjson.loads(row["bids"])
            asks = orjson.loads(row["asks"])
            if not bids or not asks:
                invalid_books += 1
                continue
            bid_prices = [Decimal(level[0]) for level in bids]
            ask_prices = [Decimal(level[0]) for level in asks]
            quantities = [Decimal(level[1]) for level in bids + asks]
            bad_price = any(price <= 0 for price in bid_prices + ask_prices)
            bad_quantity = any(quantity < 0 for quantity in quantities)
            if bad_price or bad_quantity:
                invalid_books += 1
            if bid_prices != sorted(bid_prices, reverse=True):
                bid_order_errors += 1
            if ask_prices != sorted(ask_prices):
                ask_order_errors += 1
            if bid_prices[0] >= ask_prices[0]:
                crossed_books += 1
        except (InvalidOperation, TypeError, ValueError, orjson.JSONDecodeError):
            invalid_books += 1
    if invalid_books:
        errors.append(f"{invalid_books} empty or invalid books")
    if bid_order_errors:
        errors.append(f"{bid_order_errors} books with unsorted bids")
    if ask_order_errors:
        errors.append(f"{ask_order_errors} books with unsorted asks")
    if crossed_books:
        errors.append(f"{crossed_books} crossed or locked books")
    report: dict[str, Any] = {
        "path": str(path),
        "rows": len(rows),
        "metrics": {
            "actual_snapshots": len(rows),
            "missing_seconds": len(missing),
            "longest_missing_seconds": longest_missing,
            "duplicate_seconds": duplicates,
            "last_update_id_reversals": update_reversals,
        },
        "errors": errors,
        "warnings": warnings,
    }
    report["status"] = _status(errors, warnings)
    return report


def validate_date(data_dir: Path, symbol: str, date: str) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    trade_paths = sorted((data_dir / "trades" / symbol.upper() / date).glob("*.parquet"))
    book_paths = sorted((data_dir / "orderbook" / symbol.upper() / date).glob("*.parquet"))
    for path in trade_paths:
        files.append(validate_trade_file(path))
    for path in book_paths:
        files.append(validate_orderbook_file(path))
    if not files:
        return {"status": "FAIL", "errors": ["no Parquet files found"], "files": []}
    if not trade_paths:
        files.append(
            {"dataset": "trades", "status": "FAIL", "errors": ["no trade files found"]}
        )
    if not book_paths:
        files.append(
            {"dataset": "orderbook", "status": "FAIL", "errors": ["no orderbook files found"]}
        )
    statuses = {item["status"] for item in files}
    overall = "FAIL" if "FAIL" in statuses else (
        "PASS_WITH_WARNINGS" if "PASS_WITH_WARNINGS" in statuses else "PASS"
    )
    return {"status": overall, "symbol": symbol.upper(), "date": date, "files": files}
