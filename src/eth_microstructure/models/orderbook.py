from dataclasses import asdict, dataclass
from typing import Any

import orjson


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    timestamp: int
    event_time: int
    last_update_id: int
    bids: str
    asks: str

    def as_record(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_websocket(cls, message: dict[str, Any]) -> "OrderBookSnapshot":
        event_time = int(message["E"])
        return cls(
            timestamp=event_time,
            event_time=event_time,
            last_update_id=int(message["u"]),
            bids=orjson.dumps(message["b"]).decode(),
            asks=orjson.dumps(message["a"]).decode(),
        )
