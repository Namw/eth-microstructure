from typing import Any

import httpx
import pyarrow as pa
from loguru import logger

from eth_microstructure.collector.base import BaseCollector
from eth_microstructure.models import Trade
from eth_microstructure.storage import HourlyParquetWriter

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


class TradeCollector(BaseCollector):
    rest_url = "https://fapi.binance.com/fapi/v1/aggTrades"

    def __init__(self, symbol: str, writer: HourlyParquetWriter) -> None:
        super().__init__(symbol, "aggTrade")
        self.writer = writer
        self.last_id = writer.max_value()
        self._client = httpx.AsyncClient(timeout=20)

    async def handle_message(self, message: dict[str, object]) -> None:
        trade = Trade.from_websocket(message)
        if self.last_id is not None and trade.aggregate_trade_id > self.last_id + 1:
            self.stats.gaps += 1
            await self._backfill(self.last_id + 1, trade.aggregate_trade_id)
        if self.last_id is None or trade.aggregate_trade_id > self.last_id:
            self._store(trade)

    async def _backfill(self, start_id: int, stop_id: int) -> None:
        logger.warning("Trade gap detected: backfilling IDs [{}..{})", start_id, stop_id)
        next_id = start_id
        while next_id < stop_id:
            response = await self._client.get(
                self.rest_url,
                params={"symbol": self.symbol, "fromId": next_id, "limit": 1000},
            )
            response.raise_for_status()
            batch: list[dict[str, Any]] = response.json()
            relevant = [item for item in batch if int(item["a"]) < stop_id]
            if not relevant:
                raise RuntimeError(f"Binance returned no trades while filling gap at {next_id}")
            for item in relevant:
                trade = Trade.from_rest(item)
                if self.last_id is None or trade.aggregate_trade_id > self.last_id:
                    self._store(trade)
                    self.stats.backfilled += 1
            next_id = int(relevant[-1]["a"]) + 1
        logger.info("Trade gap recovered through ID {}", stop_id - 1)

    def _store(self, trade: Trade) -> None:
        self.writer.append(trade.as_record())
        self.last_id = trade.aggregate_trade_id
        self.stats.written += 1
        self.stats.last_event_time = trade.event_time

    def close(self) -> None:
        self.writer.close()

    async def aclose(self) -> None:
        await self._client.aclose()
        self.close()
