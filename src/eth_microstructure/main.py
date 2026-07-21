import asyncio
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from loguru import logger

from eth_microstructure.collector import OrderBookCollector, TradeCollector
from eth_microstructure.collector.trade_collector import TRADE_SCHEMA
from eth_microstructure.config import AppConfig
from eth_microstructure.storage import HourlyParquetWriter, ManifestWriter

ORDERBOOK_SCHEMA = pa.schema(
    [
        ("timestamp", pa.int64()),
        ("event_time", pa.int64()),
        ("last_update_id", pa.int64()),
        ("bids", pa.string()),
        ("asks", pa.string()),
    ]
)


async def _report_stats(collectors: list[TradeCollector | OrderBookCollector]) -> None:
    while True:
        await asyncio.sleep(60)
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        for collector in collectors:
            stats = collector.stats_snapshot()
            last_event = stats.pop("last_event_time")
            latency = now_ms - last_event if last_event is not None else None
            logger.info(
                "Status {} | received={} written={} wal_bytes={} queue=0 reconnects={} "
                "gaps={} backfilled={} latency_ms={} output_hour={}",
                collector.__class__.__name__,
                stats["received"],
                stats["written"],
                collector.writer.wal_size(),
                stats["reconnects"],
                stats["gaps"],
                stats["backfilled"],
                latency,
                collector.writer.current_output_hour(),
            )


async def async_main(config_path: Path = Path("config/config.yaml")) -> None:
    config = AppConfig.load(config_path)
    collectors: list[TradeCollector | OrderBookCollector] = []
    manifest = ManifestWriter(config.storage.data_dir)

    if config.trade_stream.enabled:
        writer = HourlyParquetWriter(
            config.storage.data_dir / "trades",
            config.symbol,
            TRADE_SCHEMA,
            "trade_time",
            "aggregate_trade_id",
            config.storage.wal_fsync_every,
            event_column="event_time",
            manifest=manifest,
        )
        writer.recover()
        collectors.append(TradeCollector(config.symbol, writer))
    if config.orderbook_stream.enabled:
        writer = HourlyParquetWriter(
            config.storage.data_dir / "orderbook",
            config.symbol,
            ORDERBOOK_SCHEMA,
            "timestamp",
            "timestamp",
            config.storage.wal_fsync_every,
            event_column="event_time",
            manifest=manifest,
        )
        writer.recover()
        collectors.append(OrderBookCollector(config.symbol, writer))

    if not collectors:
        logger.warning("No collectors are enabled")
        return

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    tasks = [asyncio.create_task(collector.run()) for collector in collectors]
    tasks.append(asyncio.create_task(_report_stats(collectors)))
    await stop.wait()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for collector in collectors:
        if isinstance(collector, TradeCollector):
            await collector.aclose()
        else:
            collector.close()
    logger.info("Collectors stopped; current WALs finalized")


def run() -> None:
    from eth_microstructure.cli import cli_main

    cli_main()


def run_collector(config_path: Path = Path("config/config.yaml")) -> None:
    logger.remove()
    logger.add(sys.stderr, level="INFO", enqueue=True)
    Path("logs").mkdir(exist_ok=True)
    logger.add("logs/collector.log", rotation="1 hour", retention="7 days", enqueue=True)
    asyncio.run(async_main(config_path))


if __name__ == "__main__":
    run()
