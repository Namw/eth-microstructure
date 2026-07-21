import asyncio
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Literal

import orjson
from loguru import logger
from websockets.asyncio.client import connect


@dataclass(slots=True)
class CollectorStats:
    received: int = 0
    written: int = 0
    reconnects: int = 0
    gaps: int = 0
    backfilled: int = 0
    last_event_time: int | None = None


class BaseCollector(ABC):
    websocket_root = "wss://fstream.binance.com"

    def __init__(
        self,
        symbol: str,
        stream: str,
        route: Literal["public", "market", "private"],
        message_timeout_seconds: float = 30.0,
    ) -> None:
        self.symbol = symbol.upper()
        self.route = route
        self.url = f"{self.websocket_root}/{route}/ws/{symbol.lower()}@{stream}"
        self.message_timeout_seconds = message_timeout_seconds
        self.stats = CollectorStats()

    async def run(self) -> None:
        delay = 1.0
        while True:
            try:
                logger.info("Connecting {}", self.url)
                async with connect(
                    self.url, ping_interval=20, ping_timeout=20, close_timeout=10, max_queue=4096
                ) as websocket:
                    logger.info("Connected {}", self.url)
                    delay = 1.0
                    while True:
                        raw_message = await asyncio.wait_for(
                            websocket.recv(), timeout=self.message_timeout_seconds
                        )
                        self.stats.received += 1
                        await self.handle_message(orjson.loads(raw_message))
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                self.stats.reconnects += 1
                logger.warning(
                    "No messages from {} for {:.0f}s; reconnecting in {:.1f}s",
                    self.url,
                    self.message_timeout_seconds,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)
            except Exception:
                self.stats.reconnects += 1
                logger.exception("Collector disconnected; reconnecting in {:.1f}s", delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)

    @abstractmethod
    async def handle_message(self, message: dict[str, object]) -> None: ...

    @abstractmethod
    def close(self) -> None: ...

    def stats_snapshot(self) -> dict[str, Any]:
        return asdict(self.stats)
