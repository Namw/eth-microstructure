import argparse
import asyncio
import json
import threading
import webbrowser
from pathlib import Path
from typing import Any

import orjson

from eth_microstructure.data_access import iso_utc, parse_utc
from eth_microstructure.inspection import inspect_file
from eth_microstructure.replay import UnifiedEvent, iter_unified_events, play_events
from eth_microstructure.validation import (
    validate_date,
    validate_orderbook_file,
    validate_trade_file,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="eth-microstructure")
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect = subparsers.add_parser("collect", help="start the Binance collectors")
    collect.add_argument("--config", type=Path, default=Path("config/config.yaml"))

    inspect = subparsers.add_parser("inspect", help="inspect one hourly Parquet file")
    inspect.add_argument("stream", choices=("trades", "orderbook"))
    _location_arguments(inspect, include_hour=True)
    inspect.add_argument("--limit", type=int, default=20)

    validate = subparsers.add_parser("validate", help="validate a date or hour")
    _location_arguments(validate, include_hour=False)
    validate.add_argument("--hour", type=int, choices=range(24))
    validate.add_argument("--json", action="store_true", dest="as_json")

    replay = subparsers.add_parser("replay", help="replay stored events")
    replay.add_argument("--symbol", default="ETHUSDT")
    replay.add_argument("--start", required=True)
    replay.add_argument("--end", required=True)
    replay.add_argument("--streams", default="trades,orderbook")
    replay.add_argument("--speed", type=float, default=0)
    replay.add_argument("--data-dir", type=Path, default=Path("data"))

    web = subparsers.add_parser("web", help="open the local browser console")
    web.add_argument("--host", default="127.0.0.1")
    web.add_argument("--port", type=int, default=8765)
    web.add_argument("--no-open", action="store_true")
    return parser


def _location_arguments(parser: argparse.ArgumentParser, include_hour: bool) -> None:
    parser.add_argument("--symbol", default="ETHUSDT")
    parser.add_argument("--date", required=True)
    if include_hour:
        parser.add_argument("--hour", type=int, choices=range(24), required=True)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))


def _validate_command(args: argparse.Namespace) -> dict[str, Any]:
    if args.hour is None:
        return validate_date(args.data_dir, args.symbol, args.date)
    reports: list[dict[str, Any]] = []
    for stream, function in (
        ("trades", validate_trade_file),
        ("orderbook", validate_orderbook_file),
    ):
        path = args.data_dir / stream / args.symbol.upper() / args.date / f"{args.hour:02d}.parquet"
        if path.exists():
            reports.append(function(path))
        else:
            reports.append(
                {"dataset": stream, "path": str(path), "status": "FAIL", "errors": ["file missing"]}
            )
    statuses = {report["status"] for report in reports}
    status = "FAIL" if "FAIL" in statuses else (
        "PASS_WITH_WARNINGS" if "PASS_WITH_WARNINGS" in statuses else "PASS"
    )
    return {"status": status, "symbol": args.symbol.upper(), "date": args.date, "files": reports}


def _emit_event(event: UnifiedEvent) -> None:
    identity = event.payload.get("aggregate_trade_id", event.payload.get("last_update_id", "-"))
    payload = orjson.dumps(event.payload).decode()
    print(f"{iso_utc(event.event_time)} {event.stream} id={identity} {payload}")


def cli_main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "collect":
            from eth_microstructure.main import run_collector

            run_collector(args.config)
            return 0
        if args.command == "inspect":
            result = inspect_file(
                args.data_dir, args.stream, args.symbol, args.date, args.hour, args.limit
            )
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0
        if args.command == "validate":
            result = _validate_command(args)
            if args.as_json:
                print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
            else:
                print(json.dumps(result, ensure_ascii=False, indent=2))
            return 1 if result["status"] == "FAIL" else 0
        if args.command == "replay":
            streams = {item.strip() for item in args.streams.split(",") if item.strip()}
            invalid = streams - {"trades", "orderbook"}
            if not streams or invalid:
                parser.error(f"invalid streams: {','.join(sorted(invalid)) or 'empty'}")
            start_ms, end_ms = parse_utc(args.start), parse_utc(args.end)
            if end_ms <= start_ms:
                parser.error("--end must be later than --start")
            events = iter_unified_events(args.data_dir, args.symbol, start_ms, end_ms, streams)
            try:
                asyncio.run(play_events(events, args.speed, _emit_event))
            except KeyboardInterrupt:
                print("Replay stopped")
            return 0
        if args.command == "web":
            import uvicorn

            url = f"http://{args.host}:{args.port}"
            if not args.no_open:
                threading.Timer(0.8, webbrowser.open, args=(url,)).start()
            uvicorn.run("eth_microstructure.web.app:app", host=args.host, port=args.port)
            return 0
    except (FileNotFoundError, ValueError) as exc:
        parser.error(str(exc))
    return 2
