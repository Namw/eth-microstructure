from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class Trade:
    event_time: int
    trade_time: int
    price: str
    quantity: str
    is_buyer_maker: bool
    aggregate_trade_id: int

    def as_record(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_websocket(cls, message: dict[str, Any]) -> "Trade":
        return cls(
            event_time=int(message["E"]),
            trade_time=int(message["T"]),
            price=str(message["p"]),
            quantity=str(message["q"]),
            is_buyer_maker=bool(message["m"]),
            aggregate_trade_id=int(message["a"]),
        )

    @classmethod
    def from_rest(cls, message: dict[str, Any]) -> "Trade":
        # The REST response has no event time. Trade time is the closest lossless substitute.
        trade_time = int(message["T"])
        return cls(
            event_time=trade_time,
            trade_time=trade_time,
            price=str(message["p"]),
            quantity=str(message["q"]),
            is_buyer_maker=bool(message["m"]),
            aggregate_trade_id=int(message["a"]),
        )
