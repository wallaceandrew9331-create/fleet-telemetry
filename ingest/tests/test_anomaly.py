from datetime import UTC, datetime, timedelta

from app.anomaly import AnomalyDetector
from app.models import AlertKind, Severity, Telemetry

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def tel(seq: int, **overrides) -> Telemetry:
    fields = dict(
        asset_id="SAT-00001",
        seq=seq,
        ts=T0 + timedelta(seconds=seq),
        lat=0.0,
        lon=0.0,
        alt_km=550.0,
        battery_pct=80.0,
        temp_c=20.0,
        signal_db=-70.0,
        status="nominal",
    )
    fields.update(overrides)
    return Telemetry(**fields)


def detector() -> AnomalyDetector:
    return AnomalyDetector(expected_interval=timedelta(seconds=1), stale_after_intervals=5)


def kinds(alerts) -> list[AlertKind]:
    return [a.kind for a in alerts]


def test_nominal_stream_raises_nothing():
    d = detector()
    assert all(d.observe(tel(i)) == [] for i in range(1, 50))


def test_seq_gap_reports_missed_count():
    d = detector()
    d.observe(tel(1))
    alerts = d.observe(tel(5))
    assert kinds(alerts) == [AlertKind.SEQ_GAP]
    assert "missed 3" in alerts[0].message


def test_duplicate_and_reordered_messages_are_ignored():
    d = detector()
    d.observe(tel(10))
    assert d.observe(tel(10, battery_pct=1.0)) == []
    assert d.observe(tel(7, battery_pct=1.0)) == []


def test_threshold_alert_is_edge_triggered():
    d = detector()
    first = d.observe(tel(1, temp_c=60.0))
    assert kinds(first) == [AlertKind.OVER_TEMP]
    assert first[0].severity is Severity.WARNING
    # Still hot: no repeat.
    assert d.observe(tel(2, temp_c=65.0)) == []
    # Cools, then overheats again: fires again.
    assert d.observe(tel(3, temp_c=30.0)) == []
    assert kinds(d.observe(tel(4, temp_c=75.0))) == [AlertKind.OVER_TEMP]


def test_battery_severity_escalates_by_level():
    d = detector()
    assert d.observe(tel(1, battery_pct=5.0))[0].severity is Severity.CRITICAL


def test_safe_mode_status_alerts():
    d = detector()
    assert AlertKind.SAFE_MODE in kinds(d.observe(tel(1, status="safe_mode")))


def test_stale_asset_alerts_once_and_rearms_after_contact():
    d = detector()
    d.observe(tel(1))
    last_seen = T0 + timedelta(seconds=1)

    assert d.check_stale(last_seen + timedelta(seconds=3)) == []
    lost = d.check_stale(last_seen + timedelta(seconds=10))
    assert kinds(lost) == [AlertKind.LOST_CONTACT]
    assert d.check_stale(last_seen + timedelta(seconds=20)) == []

    d.observe(tel(2, ts=last_seen + timedelta(seconds=21)))
    assert kinds(d.check_stale(last_seen + timedelta(seconds=40))) == [AlertKind.LOST_CONTACT]


def test_seed_prevents_false_gap_after_restart():
    d = detector()
    d.seed("SAT-00001", last_seq=100, last_seen=T0)
    assert d.observe(tel(101)) == []
