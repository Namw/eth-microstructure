# ETH Microstructure Phase 1.5

Binance USD-M Futures `ETHUSDT` 原始微观数据采集器。当前版本只采集逐笔聚合成交和每秒
Top 20 订单簿快照，并提供数据检查、质量验证和终端重放。不包含 Feature Engine、Event
Detector、策略、回测、数据库、预测或 GUI。

## 数据可靠性

- WebSocket 断线后指数退避自动重连。
- Trade 根据 `aggregate_trade_id` 检测缺口，并调用 Binance Futures `aggTrades`
  REST 接口补齐后再写入实时成交。
- 每条记录先追加到 WAL；默认每条 `fsync`。小时结束后通过临时文件和原子重命名生成
  Parquet，进程重启会恢复遗留 WAL。
- 正常退出时会将当前小时也生成 Parquet。强制终止时，当前小时保留为隐藏 WAL，重启后继续。

注意：本方案防范网络断线和普通进程崩溃；磁盘损坏、Binance 历史接口自身缺失或超过交易所
保留期限不可能由单机采集器保证。

## 初始化

要求安装 `uv`，其余 Python 和依赖均由 uv 管理：

```bash
uv python install 3.12
uv python pin 3.12
uv sync
```

项目使用 src layout。运行依赖与 `[dependency-groups].dev` 开发依赖分别记录在
`pyproject.toml`，锁定结果提交在 `uv.lock`。

## 采集

编辑 `config/config.yaml` 后，在项目根目录执行：

```bash
uv run eth-microstructure collect
```

用 `Ctrl-C` 正常退出。输出结构为：

```text
data/
  trades/ETHUSDT/2026-07-21/20.parquet
  orderbook/ETHUSDT/2026-07-21/20.parquet
  metadata/manifest.jsonl
```

时间戳均为 UTC Unix 毫秒，日期和小时目录也按 UTC 划分。`bids`、`asks` 是保留交易所
字符串精度的 JSON 字符串。价格和数量同样以字符串存储，避免采集阶段发生浮点精度损失。
OrderBook schema v2 同时保留原 `timestamp`，并增加同源的 `event_time` 和 Binance
`last_update_id`。旧三列 Parquet 仍可检查、验证和重放，但 update ID 检查会报告 warning。

采集过程每 60 秒输出收到/写入记录数、WAL 大小、队列长度、重连和 Trade 补洞统计、
最近事件延迟及当前 UTC 输出小时，不再逐笔打印全部成交。

Manifest 在 Parquet 原子落盘成功后追加，包含行数、时间范围、文件大小、SHA-256 和 ID
范围。它是可重建的辅助索引；所有读取命令仍会直接发现 Parquet，Manifest 损坏或写入失败
不会影响主数据落盘。

## Inspect

读取单个小时文件，不修改原数据：

```bash
uv run eth-microstructure inspect trades \
  --symbol ETHUSDT --date 2026-07-21 --hour 20 --limit 20

uv run eth-microstructure inspect orderbook \
  --symbol ETHUSDT --date 2026-07-21 --hour 20 --limit 20
```

Trade 输出主动方向和读取时计算的 `quote_amount`。OrderBook 解析 JSON，并输出一档价格、
spread、mid price 和 Top20 双边基础资产数量深度。所有时间同时输出 Unix 毫秒和 UTC ISO 8601。

## Validate

```bash
uv run eth-microstructure validate --symbol ETHUSDT --date 2026-07-21
uv run eth-microstructure validate --symbol ETHUSDT --date 2026-07-21 --hour 20
uv run eth-microstructure validate --symbol ETHUSDT --date 2026-07-21 --json
```

状态为 `PASS`、`PASS_WITH_WARNINGS` 或 `FAIL`。检查覆盖成交 ID 连续性、时间顺序、
价格数量、UTC 文件小时、分钟计数、无成交区间，以及快照缺秒/重复、更新 ID、档位排序、
交叉盘口和空/非法盘口。`--json` 输出紧凑的机器可读 JSON。检查是只读的。

## Replay

```bash
uv run eth-microstructure replay \
  --symbol ETHUSDT \
  --start 2026-07-21T20:00:00Z \
  --end 2026-07-21T20:05:00Z \
  --streams trades,orderbook \
  --speed 0
```

`--speed 0` 不等待，`1` 为真实时间，`10` 为十倍速。可只选择 `trades` 或
`orderbook`。实现按小时读取，在每个小时内合并并确定性排序，不会一次加载完整日期。
`Ctrl-C` 可正常停止。`UnifiedEvent`/`stream_events` 是与 Collector 解耦的统一消费接口。

限制：Replay v1 只输出终端文本；Validate 不修复数据；日期检查要求 Trade 和 OrderBook
两类数据均存在，但不会假定一天必须有 24 个小时文件，也不会通过 Manifest 推断缺失小时。

## 验证与开发

```bash
uv run pytest
uv run ruff check .
uv run mypy
```

快速读取一个文件：

```bash
uv run python -c 'import pyarrow.parquet as pq; print(pq.read_table("data/trades/ETHUSDT/2026-07-21/20.parquet"))'
```

## 模块边界

```text
src/eth_microstructure/
  collector/   # 连接、重连和各数据流行为
  models/      # 原始消息到稳定记录的映射
  storage/     # 与数据源无关的 WAL + Parquet 小时轮转
  inspection.py
  validation.py
  replay.py
  cli.py
  config.py
  main.py      # 只负责装配和生命周期
```

未来新增 OI、Funding 或 Liquidation 时，可增加 model 和 collector，再在 `main.py` 装配；
现有采集器和存储实现无需修改。
