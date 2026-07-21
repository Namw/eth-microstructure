import asyncio
import fcntl
import json
import os
import signal
import sys
from collections.abc import AsyncIterator
from contextlib import suppress
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from eth_microstructure.data_access import parse_utc
from eth_microstructure.inspection import inspect_file
from eth_microstructure.replay import iter_unified_events, stream_events
from eth_microstructure.validation import validate_date

PROJECT_ROOT = Path.cwd()
DATA_DIR = PROJECT_ROOT / "data"
LOG_PATH = PROJECT_ROOT / "logs" / "collector.log"
LOCK_PATH = PROJECT_ROOT / "logs" / "collector.lock"
STATIC_DIR = Path(__file__).parent / "static"


class CollectorProcess:
    def __init__(self) -> None:
        self.process: asyncio.subprocess.Process | None = None
        self._operation_lock = asyncio.Lock()

    @property
    def managed_running(self) -> bool:
        return self.process is not None and self.process.returncode is None

    def active_pid(self) -> int | None:
        if self.managed_running and self.process is not None:
            return self.process.pid
        if not LOCK_PATH.exists():
            return None
        with LOCK_PATH.open("a+") as lock_file:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                lock_file.seek(0)
                value = lock_file.read().strip()
                return int(value) if value.isdigit() else None
            finally:
                with suppress(OSError):
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        return None

    @property
    def running(self) -> bool:
        return self.active_pid() is not None

    async def start(self) -> None:
        async with self._operation_lock:
            await self._start()

    async def _start(self) -> None:
        if self.running:
            raise RuntimeError("Collector is already running")
        self.process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "eth_microstructure.main",
            "collect",
            cwd=PROJECT_ROOT,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
        for _ in range(20):
            await asyncio.sleep(0.05)
            if self.process.returncode is not None:
                raise RuntimeError("Collector exited immediately; check logs/collector.log")
            if self.active_pid() is not None:
                return
        raise RuntimeError("Collector did not acquire its lock; check logs/collector.log")

    async def stop(self) -> None:
        async with self._operation_lock:
            await self._stop()

    async def _stop(self) -> None:
        pid = self.active_pid()
        if pid is None:
            return
        if self.managed_running and self.process is not None:
            os.killpg(pid, signal.SIGINT)
            try:
                await asyncio.wait_for(self.process.wait(), timeout=20)
            except TimeoutError:
                os.killpg(pid, signal.SIGTERM)
                await self.process.wait()
            return
        os.kill(pid, signal.SIGINT)
        for _ in range(80):
            if self.active_pid() is None:
                return
            await asyncio.sleep(0.25)
        raise RuntimeError("Collector did not stop within 20 seconds")

    async def restart(self) -> None:
        async with self._operation_lock:
            await self._stop()
            await self._start()


manager = CollectorProcess()

# The collector is intentionally independent of the web server lifecycle. Restarting
# the console (or opening/closing browser tabs) must not interrupt data collection.
app = FastAPI(title="ETH Microstructure Console")


class InspectRequest(BaseModel):
    stream: str
    symbol: str = "ETHUSDT"
    date: str
    hour: int = Field(ge=0, le=23)
    limit: int = Field(default=20, ge=0, le=200)


class ValidateRequest(BaseModel):
    symbol: str = "ETHUSDT"
    date: str


def _files() -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    for stream in ("trades", "orderbook"):
        root = DATA_DIR / stream
        for path in sorted(root.glob("*/*/*.parquet"), reverse=True):
            try:
                rows = pq.ParquetFile(path).metadata.num_rows
            except Exception:
                rows = None
            files.append(
                {
                    "stream": stream,
                    "symbol": path.parent.parent.name,
                    "date": path.parent.name,
                    "hour": int(path.stem),
                    "path": str(path.relative_to(DATA_DIR.parent)),
                    "size": path.stat().st_size,
                    "rows": rows,
                }
            )
    return sorted(files, key=lambda item: (item["date"], item["hour"]), reverse=True)


def _active_wals() -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    for stream in ("trades", "orderbook"):
        for path in sorted((DATA_DIR / stream).glob("*/*/.*.wal"), reverse=True):
            with path.open("rb") as file:
                rows = sum(1 for line in file if line.endswith(b"\n") and line.strip())
            files.append(
                {
                    "stream": stream,
                    "symbol": path.parent.parent.name,
                    "date": path.parent.name,
                    "hour": int(path.stem[1:]),
                    "path": str(path.relative_to(DATA_DIR.parent)),
                    "size": path.stat().st_size,
                    "rows": rows,
                    "active": True,
                }
            )
    return files


def _tail_log(lines: int = 30) -> list[str]:
    if not LOG_PATH.exists():
        return []
    with LOG_PATH.open(errors="replace") as file:
        return file.readlines()[-lines:]


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/app.css")
async def stylesheet() -> FileResponse:
    return FileResponse(STATIC_DIR / "app.css", media_type="text/css")


@app.get("/app.js")
async def javascript() -> FileResponse:
    return FileResponse(STATIC_DIR / "app.js", media_type="text/javascript")


@app.get("/api/status")
async def status() -> dict[str, Any]:
    files = _files()
    active_wals = _active_wals()
    visible = sorted(
        files + active_wals,
        key=lambda item: (item["date"], item["hour"]),
        reverse=True,
    )
    hours = {
        (item["stream"], item["symbol"], item["date"], item["hour"])
        for item in visible
    }
    return {
        "collector_running": manager.running,
        "collector_pid": manager.active_pid(),
        "collector_managed_by_web": manager.managed_running,
        "parquet_files": len(files),
        "trade_files": sum(item[0] == "trades" for item in hours),
        "orderbook_files": sum(item[0] == "orderbook" for item in hours),
        "active_wal_files": len(active_wals),
        "wal_bytes": sum(item["size"] for item in active_wals),
        "latest_files": visible[:12],
        "logs": _tail_log(),
    }


@app.post("/api/collector/start")
async def start_collector() -> dict[str, Any]:
    try:
        await manager.start()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"running": True, "pid": manager.process.pid if manager.process else None}


@app.post("/api/collector/stop")
async def stop_collector() -> dict[str, bool]:
    try:
        await manager.stop()
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"running": False}


@app.post("/api/collector/restart")
async def restart_collector() -> dict[str, Any]:
    try:
        await manager.restart()
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"running": True, "pid": manager.active_pid()}


@app.post("/api/inspect")
async def inspect_data(request: InspectRequest) -> dict[str, Any]:
    if request.stream not in {"trades", "orderbook"}:
        raise HTTPException(status_code=400, detail="stream must be trades or orderbook")
    try:
        return inspect_file(
            DATA_DIR,
            request.stream,
            request.symbol,
            request.date,
            request.hour,
            request.limit,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/validate")
async def validate_data(request: ValidateRequest) -> dict[str, Any]:
    return validate_date(DATA_DIR, request.symbol, request.date)


@app.get("/api/replay")
async def replay_data(
    symbol: str = "ETHUSDT",
    start: str = Query(...),
    end: str = Query(...),
    streams: str = "trades,orderbook",
    speed: float = Query(default=0, ge=0),
) -> StreamingResponse:
    selected = {item.strip() for item in streams.split(",") if item.strip()}
    if not selected or selected - {"trades", "orderbook"}:
        raise HTTPException(status_code=400, detail="invalid streams")
    try:
        start_ms, end_ms = parse_utc(start), parse_utc(end)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if end_ms <= start_ms:
        raise HTTPException(status_code=400, detail="end must be later than start")

    async def generate() -> AsyncIterator[str]:
        events = iter_unified_events(DATA_DIR, symbol, start_ms, end_ms, selected)
        async for event in stream_events(events, speed):
            body = {
                "event_time": event.event_time,
                "stream": event.stream,
                "payload": event.payload,
            }
            yield f"data: {json.dumps(body, separators=(',', ':'))}\n\n"
        yield "event: complete\ndata: {}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")
