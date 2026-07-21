from pathlib import Path

import orjson
import pyarrow as pa
from conftest import write_rows
from fastapi.testclient import TestClient

from eth_microstructure.web import app as web_app

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
START = 1_784_664_000_000


def _trade() -> dict[str, object]:
    return {
        "event_time": START,
        "trade_time": START,
        "price": "2",
        "quantity": "3",
        "is_buyer_maker": False,
        "aggregate_trade_id": 42,
    }


def test_console_and_status(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.setattr(web_app, "DATA_DIR", tmp_path)
    with TestClient(web_app.app) as client:
        assert client.get("/").status_code == 200
        status = client.get("/api/status")
    assert status.status_code == 200
    assert status.json()["parquet_files"] == 0


def test_inspect_and_replay_api(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.setattr(web_app, "DATA_DIR", tmp_path)
    path = tmp_path / "trades/ETHUSDT/2026-07-21/20.parquet"
    write_rows(path, [_trade()], TRADE_SCHEMA)
    with TestClient(web_app.app) as client:
        inspected = client.post(
            "/api/inspect",
            json={
                "stream": "trades",
                "symbol": "ETHUSDT",
                "date": "2026-07-21",
                "hour": 20,
                "limit": 1,
            },
        )
        replayed = client.get(
            "/api/replay",
            params={
                "symbol": "ETHUSDT",
                "start": "2026-07-21T20:00:00Z",
                "end": "2026-07-21T20:01:00Z",
                "streams": "trades",
                "speed": 0,
            },
        )
    assert inspected.status_code == 200
    assert inspected.json()["samples"][0]["quote_amount"] == "6"
    assert replayed.status_code == 200
    assert '"aggregate_trade_id":42' in replayed.text
    assert "event: complete" in replayed.text


def test_status_inspect_and_replay_include_active_wal(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.setattr(web_app, "DATA_DIR", tmp_path)
    path = tmp_path / "trades/ETHUSDT/2026-07-21/.20.wal"
    path.parent.mkdir(parents=True)
    path.write_bytes(orjson.dumps(_trade()) + b"\n")
    with TestClient(web_app.app) as client:
        status = client.get("/api/status").json()
        inspected = client.post("/api/inspect", json={
            "stream": "trades", "symbol": "ETHUSDT", "date": "2026-07-21",
            "hour": 20, "limit": 1,
        })
        replayed = client.get("/api/replay", params={
            "symbol": "ETHUSDT", "start": "2026-07-21T20:00:00Z",
            "end": "2026-07-21T20:01:00Z", "streams": "trades", "speed": 0,
        })
    assert status["active_wal_files"] == 1
    assert status["trade_files"] == 1
    assert status["latest_files"][0]["rows"] == 1
    assert inspected.json()["rows"] == 1
    assert '"aggregate_trade_id":42' in replayed.text
