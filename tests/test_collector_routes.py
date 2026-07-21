from eth_microstructure.collector.base import BaseCollector


class DummyCollector(BaseCollector):
    async def handle_message(self, message: dict[str, object]) -> None:
        return None

    def close(self) -> None:
        return None


def test_market_route_url() -> None:
    collector = DummyCollector("ETHUSDT", "aggTrade", route="market")
    assert collector.url == "wss://fstream.binance.com/market/ws/ethusdt@aggTrade"


def test_public_route_url() -> None:
    collector = DummyCollector("ETHUSDT", "depth20@100ms", route="public")
    assert collector.url == "wss://fstream.binance.com/public/ws/ethusdt@depth20@100ms"
