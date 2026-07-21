from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field


class StreamConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True


class StorageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    data_dir: Path = Path("data")
    wal_fsync_every: int = Field(default=1, ge=1)


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    symbol: str = "ETHUSDT"
    trade_stream: StreamConfig = StreamConfig()
    orderbook_stream: StreamConfig = StreamConfig()
    rotation: Literal["hourly"] = "hourly"
    output: Literal["parquet"] = "parquet"
    storage: StorageConfig = StorageConfig()

    @classmethod
    def load(cls, path: Path) -> "AppConfig":
        with path.open("rb") as file:
            return cls.model_validate(yaml.safe_load(file) or {})
