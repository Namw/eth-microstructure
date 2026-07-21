import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from eth_microstructure.data_access import read_hour_rows


@dataclass(frozen=True, slots=True)
class UnifiedEvent:
    event_time: int
    stream: str
    sequence: int
    payload: dict[str, Any]


def replay_delay(previous_ms: int | None, current_ms: int, speed: float) -> float:
    if speed < 0:
        raise ValueError("speed must be >= 0")
    if speed == 0 or previous_ms is None:
        return 0.0
    return max(0, current_ms - previous_ms) / 1000 / speed


def _row_events(
    rows: list[dict[str, Any]], stream: str, start_ms: int, end_ms: int
) -> list[UnifiedEvent]:
    event_column = "event_time" if not rows or "event_time" in rows[0] else "timestamp"
    events: list[UnifiedEvent] = []
    for sequence, row in enumerate(rows):
        event_time = int(row[event_column])
        if start_ms <= event_time < end_ms:
            events.append(UnifiedEvent(event_time, stream, sequence, row))
    return events


def iter_unified_events(
    data_dir: Path,
    symbol: str,
    start_ms: int,
    end_ms: int,
    streams: set[str],
) -> Iterator[UnifiedEvent]:
    """Yield deterministic event-time order while holding at most one hour in memory."""
    moment = datetime.fromtimestamp(start_ms / 1000, UTC).replace(minute=0, second=0, microsecond=0)
    end = datetime.fromtimestamp((end_ms - 1) / 1000, UTC)
    stream_rank = {"trades": 0, "orderbook": 1}
    while moment <= end:
        events: list[UnifiedEvent] = []
        for stream in sorted(streams, key=stream_rank.__getitem__):
            rows, _ = read_hour_rows(
                data_dir, stream, symbol, moment.strftime("%Y-%m-%d"), moment.hour
            )
            events.extend(_row_events(rows, stream, start_ms, end_ms))
        events.sort(
            key=lambda event: (
                event.event_time,
                stream_rank[event.stream],
                int(
                    event.payload.get(
                        "aggregate_trade_id", event.payload.get("last_update_id", event.sequence)
                    )
                ),
                event.sequence,
            )
        )
        yield from events
        moment += timedelta(hours=1)


async def play_events(
    events: Iterable[UnifiedEvent],
    speed: float,
    emit: Callable[[UnifiedEvent], None],
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> int:
    previous: int | None = None
    count = 0
    for event in events:
        delay = replay_delay(previous, event.event_time, speed)
        if delay:
            await sleep(delay)
        emit(event)
        previous = event.event_time
        count += 1
    return count


async def stream_events(
    events: Iterable[UnifiedEvent], speed: float
) -> AsyncIterator[UnifiedEvent]:
    previous: int | None = None
    for event in events:
        delay = replay_delay(previous, event.event_time, speed)
        if delay:
            await asyncio.sleep(delay)
        yield event
        previous = event.event_time
