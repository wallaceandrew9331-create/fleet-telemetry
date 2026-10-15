from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .anomaly import AnomalyDetector, StreamClock
from .config import settings
from .consumer import run_broadcaster, run_consumer, run_stale_checker
from .db import Repository
from .hub import FleetHub
from .models import Telemetry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("ingest")


@asynccontextmanager
async def lifespan(app: FastAPI):
    repo = await Repository.connect(settings.database_url)
    hub = FleetHub()
    detector = AnomalyDetector(settings.expected_interval)
    clock = StreamClock()

    # Warm state from the database so a restart doesn't blank the dashboard
    # or misreport every asset's first message as a sequence gap.
    for row in await repo.load_assets():
        detector.seed(row["asset_id"], row["last_seq"], row["last_seen"])
        hub.apply([Telemetry(seq=row["last_seq"], ts=row["last_seen"], **_asset_fields(row))], [])
    hub.take_delta()
    log.info("restored %d assets from database", len(hub.assets))

    app.state.repo, app.state.hub = repo, hub
    tasks = [
        asyncio.create_task(run_consumer(settings, repo, hub, detector, clock), name="consumer"),
        asyncio.create_task(run_stale_checker(settings, repo, hub, detector, clock), name="stale-checker"),
        asyncio.create_task(run_broadcaster(settings, hub), name="broadcaster"),
    ]
    for t in tasks:
        t.add_done_callback(_crash_on_failure)
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await repo.close()


def _asset_fields(row) -> dict:
    keys = ("asset_id", "lat", "lon", "alt_km", "battery_pct", "temp_c", "signal_db", "status")
    return {k: row[k] for k in keys}


def _crash_on_failure(task: asyncio.Task) -> None:
    # A dead background task means a silently stale dashboard; fail loudly so
    # the orchestrator restarts the container instead.
    if not task.cancelled() and task.exception() is not None:
        log.critical("background task %s died", task.get_name(), exc_info=task.exception())
        raise SystemExit(1)


app = FastAPI(title="Fleet Telemetry Ingest", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True, "assets": len(app.state.hub.assets)}


@app.get("/metrics")
async def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/assets")
async def list_assets() -> list[dict]:
    return list(app.state.hub.assets.values())


@app.get("/api/assets/{asset_id}/history")
async def asset_history(asset_id: str, minutes: int = Query(15, ge=1, le=24 * 60)) -> list[dict]:
    if asset_id not in app.state.hub.assets:
        raise HTTPException(404, "unknown asset")
    since = datetime.now(UTC) - timedelta(minutes=minutes)
    rows = await app.state.repo.asset_history(asset_id, since)
    return [dict(r) for r in rows]


@app.get("/api/alerts")
async def list_alerts(limit: int = Query(100, ge=1, le=1000)) -> list[dict]:
    return [dict(r) for r in await app.state.repo.recent_alerts(limit)]


@app.websocket("/ws")
async def fleet_stream(ws: WebSocket) -> None:
    await ws.accept()
    hub: FleetHub = app.state.hub
    queue = hub.subscribe()
    try:
        await ws.send_json(hub.snapshot())
        while True:
            message = await queue.get()
            if message is None:
                await ws.close(code=1013, reason="client too slow")
                return
            await ws.send_json(message)
    except WebSocketDisconnect:
        pass
    finally:
        hub.unsubscribe(queue)
