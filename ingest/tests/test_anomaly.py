from datetime import UTC, datetime, timedelta

from app.anomaly import AnomalyDetector, StreamClock
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


def test_reboot_with_seq_reset_rebaselines_instead_of_dropping():
    d = detector()
    d.observe(tel(500))
    reboot = d.observe(tel(1, ts=T0 + timedelta(seconds=600)))
    assert kinds(reboot) == [AlertKind.RESTART]
    assert "500 to 1" in reboot[0].message
    # Subsequent messages are processed normally, with gap detection intact.
    assert d.observe(tel(2, ts=T0 + timedelta(seconds=601))) == []
    assert kinds(d.observe(tel(5, ts=T0 + timedelta(seconds=604)))) == [AlertKind.SEQ_GAP]


def test_seed_prevents_false_gap_after_restart():
    d = detector()
    d.seed("SAT-00001", last_seq=100, last_seen=T0)
    assert d.observe(tel(101)) == []


def test_stream_clock_tracks_wall_time_when_consumer_is_current():
    clock = StreamClock()
    wall = T0 + timedelta(seconds=100)
    assert clock.now(wall) == wall  # nothing processed yet
    clock.advance(wall - timedelta(milliseconds=20), wall)
    assert clock.now(wall + timedelta(seconds=1)) == wall + timedelta(seconds=1) - timedelta(milliseconds=20)


def test_stream_clock_keeps_running_when_feed_stops():
    clock = StreamClock()
    clock.advance(T0, T0)
    assert clock.now(T0 + timedelta(seconds=30)) == T0 + timedelta(seconds=30)


def test_stream_clock_ignores_events_from_the_future():
    clock = StreamClock()
    clock.advance(T0 + timedelta(hours=1), T0)  # one asset with a fast clock
    assert clock.now(T0) == T0


def test_backlogged_consumer_does_not_report_whole_fleet_lost():
    # Regression: with the consumer 60 s behind, wall-clock staleness reported
    # every asset as lost on every check (4.3M false alerts in a 20k msg/s run).
    d, clock = detector(), StreamClock()
    lag = timedelta(seconds=60)
    for step in range(1, 20):
        event_ts = T0 + timedelta(seconds=step)
        for i in range(100):
            d.observe(tel(step, asset_id=f"SAT-{i:05d}", ts=event_ts))
        clock.advance(event_ts, event_ts + lag)
        wall = event_ts + lag + timedelta(milliseconds=500)
        assert d.check_stale(clock.now(wall)) == []
        assert d.check_stale(wall) != []  # what the old wall-clock check did


def test_silent_asset_is_still_detected_while_others_report():
    d, clock = detector(), StreamClock()
    lag = timedelta(seconds=60)
    d.observe(tel(1, asset_id="SAT-QUIET", ts=T0))
    lost = []
    for step in range(1, 20):
        event_ts = T0 + timedelta(seconds=step)
        d.observe(tel(step, ts=event_ts))
        clock.advance(event_ts, event_ts + lag)
        lost += d.check_stale(clock.now(event_ts + lag))
    assert [(a.asset_id, a.kind) for a in lost] == [("SAT-QUIET", AlertKind.LOST_CONTACT)]
