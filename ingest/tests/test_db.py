from datetime import UTC, datetime, timedelta

from app.db import as_columns, latest_per_asset, split_known
from app.models import TELEMETRY_COLUMNS, Telemetry

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def tel(asset_id: str, seconds: int, seq: int = 1) -> Telemetry:
    return Telemetry(
        asset_id=asset_id,
        seq=seq,
        ts=T0 + timedelta(seconds=seconds),
        lat=1.0,
        lon=2.0,
        alt_km=550.0,
        battery_pct=80.0,
        temp_c=20.0,
        signal_db=-70.0,
        status="nominal",
    )


def test_latest_per_asset_keeps_newest_timestamp_not_arrival_order():
    rows = latest_per_asset([tel("A", 5, seq=5), tel("B", 1), tel("A", 3, seq=3)])
    assert {(t.asset_id, t.seq) for t in rows} == {("A", 5), ("B", 1)}


def test_latest_per_asset_prefers_rebooted_asset_with_newer_timestamp():
    rows = latest_per_asset([tel("A", 5, seq=900), tel("A", 6, seq=1)])
    assert [t.seq for t in rows] == [1]


def test_split_known_partitions_by_membership():
    existing, new = split_known([tel("A", 1), tel("B", 1), tel("C", 1)], known={"A", "C"})
    assert [t.asset_id for t in existing] == ["A", "C"]
    assert [t.asset_id for t in new] == ["B"]


def test_as_columns_matches_unnest_parameter_order():
    cols = as_columns([tel("A", 1, seq=7), tel("B", 2, seq=8)])
    assert len(cols) == len(TELEMETRY_COLUMNS)
    by_name = dict(zip(TELEMETRY_COLUMNS, cols, strict=True))
    assert by_name["asset_id"] == ["A", "B"]
    assert by_name["seq"] == [7, 8]
    assert by_name["ts"] == [T0 + timedelta(seconds=1), T0 + timedelta(seconds=2)]
