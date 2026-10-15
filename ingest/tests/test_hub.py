import asyncio
from datetime import UTC, datetime, timedelta

from app.consumer import parse_batch
from app.hub import CLIENT_QUEUE_DEPTH, FleetHub
from app.models import Alert, AlertKind, Severity, Telemetry


def tel(asset_id: str, seq: int, battery: float = 80.0, at: int = 0) -> Telemetry:
    return Telemetry(
        asset_id=asset_id,
        seq=seq,
        ts=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=at),
        lat=1.0,
        lon=2.0,
        alt_km=550.0,
        battery_pct=battery,
        temp_c=20.0,
        signal_db=-70.0,
        status="nominal",
    )


def test_delta_coalesces_many_updates_per_asset():
    hub = FleetHub()
    hub.apply([tel("A", i, battery=i) for i in range(1, 100)], [])
    delta = hub.take_delta()
    assert len(delta["assets"]) == 1
    assert delta["assets"][0]["battery_pct"] == 99
    assert hub.take_delta() is None


def test_older_update_does_not_overwrite_newer_state():
    hub = FleetHub()
    hub.apply([tel("A", 5, battery=10, at=5)], [])
    hub.apply([tel("A", 3, battery=99, at=3)], [])
    assert hub.assets["A"]["battery_pct"] == 10


def test_rebooted_asset_with_reset_seq_still_updates():
    hub = FleetHub()
    hub.apply([tel("A", 500, battery=10, at=500)], [])
    hub.apply([tel("A", 1, battery=99, at=600)], [])
    assert hub.assets["A"]["seq"] == 1


def test_alerts_flow_into_delta_and_recent_list():
    hub = FleetHub()
    alert = Alert("A", AlertKind.SAFE_MODE, Severity.CRITICAL, "safe mode")
    hub.apply([], [alert])
    assert hub.take_delta()["alerts"][0]["kind"] == "safe_mode"
    assert hub.snapshot()["alerts"][0]["asset_id"] == "A"


def test_slow_client_is_dropped_with_close_sentinel():
    hub = FleetHub()
    q = hub.subscribe()
    for i in range(CLIENT_QUEUE_DEPTH + 1):
        hub.broadcast({"n": i})
    assert hub.client_count == 0
    assert hub.dropped_clients == 1
    assert q.qsize() == 1 and q.get_nowait() is None


def test_parse_batch_rejects_malformed_messages():
    good = tel("A", 1).model_dump_json().encode()
    batch, rejected = parse_batch([good, b"not json", b'{"asset_id": "A"}'])
    assert len(batch) == 1
    assert rejected == 2


def test_subscribe_returns_bounded_queue():
    q = FleetHub().subscribe()
    assert isinstance(q, asyncio.Queue) and q.maxsize == CLIENT_QUEUE_DEPTH
