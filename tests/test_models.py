import orjson

from eth_microstructure.models import OrderBookSnapshot, Trade


def test_trade_from_websocket() -> None:
    trade = Trade.from_websocket({"E": 10, "T": 9, "p": "1.2", "q": "3", "m": True, "a": 7})
    assert trade.aggregate_trade_id == 7
    assert trade.price == "1.2"


def test_orderbook_serializes_levels_as_json() -> None:
    snapshot = OrderBookSnapshot.from_websocket(
        {"E": 1000, "u": 99, "b": [["1", "2"]], "a": [["3", "4"]]}
    )
    assert orjson.loads(snapshot.bids) == [["1", "2"]]
    assert snapshot.last_update_id == 99
