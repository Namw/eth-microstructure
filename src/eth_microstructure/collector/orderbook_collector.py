from eth_microstructure.collector.base import BaseCollector
from eth_microstructure.models import OrderBookSnapshot
from eth_microstructure.storage import HourlyParquetWriter


class OrderBookCollector(BaseCollector):
    def __init__(self, symbol: str, writer: HourlyParquetWriter) -> None:
        super().__init__(symbol, "depth20@100ms", route="public")
        self.writer = writer
        latest_timestamp = writer.max_value()
        self._last_second = latest_timestamp // 1000 if latest_timestamp is not None else None

    async def handle_message(self, message: dict[str, object]) -> None:
        snapshot = OrderBookSnapshot.from_websocket(message)
        second = snapshot.timestamp // 1000
        if second == self._last_second:
            return
        self._last_second = second
        self.writer.append(snapshot.as_record())
        self.stats.written += 1
        self.stats.last_event_time = snapshot.event_time

    def close(self) -> None:
        self.writer.close()
