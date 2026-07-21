import asyncio
from pathlib import Path

import orjson
import pyarrow as pa
from conftest import write_rows

from eth_microstructure.replay import UnifiedEvent, iter_unified_events, play_events, replay_delay

TRADE_SCHEMA = pa.schema([("event_time", pa.int64()), ("aggregate_trade_id", pa.int64())])
BOOK_SCHEMA = pa.schema(
    [
        ("event_time", pa.int64()),
        ("last_update_id", pa.int64()),
        ("bids", pa.string()),
        ("asks", pa.string()),
    ]
)


def test_cross_hour_replay_is_deterministic_and_sorted(tmp_path: Path) -> None:
    base = 1_784_664_000_000
    write_rows(
        tmp_path / "trades/ETHUSDT/2026-07-21/20.parquet",
        [
            {"event_time": base + 2, "aggregate_trade_id": 2},
            {"event_time": base, "aggregate_trade_id": 1},
        ],
        TRADE_SCHEMA,
    )
    write_rows(
        tmp_path / "orderbook/ETHUSDT/2026-07-21/20.parquet",
        [{"event_time": base, "last_update_id": 9, "bids": "[]", "asks": "[]"}], BOOK_SCHEMA,
    )
    write_rows(
        tmp_path / "trades/ETHUSDT/2026-07-21/21.parquet",
        [{"event_time": base + 3_600_001, "aggregate_trade_id": 3}], TRADE_SCHEMA,
    )
    events = list(
        iter_unified_events(
            tmp_path, "ETHUSDT", base, base + 3_600_100, {"trades", "orderbook"}
        )
    )
    assert [event.event_time for event in events] == sorted(event.event_time for event in events)
    assert [event.stream for event in events[:2]] == ["trades", "orderbook"]
    assert events[-1].payload["aggregate_trade_id"] == 3


def test_speed_delay_and_playback() -> None:
    assert replay_delay(1000, 2000, 10) == 0.1
    assert replay_delay(1000, 2000, 0) == 0
    sleeps: list[float] = []
    emitted: list[int] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    events = [UnifiedEvent(1000, "trades", 0, {}), UnifiedEvent(3000, "trades", 1, {})]
    count = asyncio.run(
        play_events(events, 2, lambda event: emitted.append(event.event_time), fake_sleep)
    )
    assert count == 2
    assert sleeps == [1.0]
    assert emitted == [1000, 3000]


def test_replay_reads_active_wal(tmp_path: Path) -> None:
    base = 1_784_664_000_000
    path = tmp_path / "trades/ETHUSDT/2026-07-21/.20.wal"
    path.parent.mkdir(parents=True)
    path.write_bytes(orjson.dumps({"event_time": base, "aggregate_trade_id": 7}) + b"\n")
    events = list(iter_unified_events(tmp_path, "ETHUSDT", base, base + 1000, {"trades"}))
    assert [event.payload["aggregate_trade_id"] for event in events] == [7]
