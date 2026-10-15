"""Stateful anomaly detection over the telemetry stream.

Pure logic with no I/O so it can be unit tested and profiled in isolation.
Threshold alerts are edge-triggered: an alert fires when a condition starts,
and can fire again only after the condition has cleared. Without that, one
overheating satellite would emit an alert per message.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .models import Alert, AlertKind, Severity, Telemetry

BATTERY_WARN = 20.0
BATTERY_CRIT = 10.0
TEMP_WARN = 55.0
TEMP_CRIT = 70.0


@dataclass
class _AssetState:
    last_seq: int
    last_seen: datetime
    active: set[AlertKind] = field(default_factory=set)


class AnomalyDetector:
    def __init__(self, expected_interval: timedelta, stale_after_intervals: int = 5):
        self.stale_after = expected_interval * stale_after_intervals
        self._assets: dict[str, _AssetState] = {}

    def observe(self, t: Telemetry) -> list[Alert]:
        state = self._assets.get(t.asset_id)
        alerts: list[Alert] = []

        if state is None:
            state = self._assets[t.asset_id] = _AssetState(last_seq=t.seq, last_seen=t.ts)
        else:
            if t.seq <= state.last_seq:
                # Duplicate or reordered delivery; never let it rewind state.
                return []
            missed = t.seq - state.last_seq - 1
            if missed > 0:
                alerts.append(
                    Alert(
                        t.asset_id,
                        AlertKind.SEQ_GAP,
                        Severity.WARNING,
                        f"missed {missed} message(s) before seq {t.seq}",
                        t.ts,
                    )
                )
            state.last_seq = t.seq
            state.last_seen = t.ts

        if AlertKind.LOST_CONTACT in state.active:
            state.active.discard(AlertKind.LOST_CONTACT)

        self._edge(
            state,
            alerts,
            t,
            AlertKind.LOW_BATTERY,
            t.battery_pct < BATTERY_WARN,
            Severity.CRITICAL if t.battery_pct < BATTERY_CRIT else Severity.WARNING,
            f"battery at {t.battery_pct:.1f}%",
        )
        self._edge(
            state,
            alerts,
            t,
            AlertKind.OVER_TEMP,
            t.temp_c > TEMP_WARN,
            Severity.CRITICAL if t.temp_c > TEMP_CRIT else Severity.WARNING,
            f"temperature at {t.temp_c:.1f}°C",
        )
        self._edge(
            state, alerts, t, AlertKind.SAFE_MODE, t.status == "safe_mode", Severity.CRITICAL, "asset entered safe mode"
        )
        return alerts

    def check_stale(self, now: datetime) -> list[Alert]:
        alerts = []
        for asset_id, state in self._assets.items():
            if AlertKind.LOST_CONTACT in state.active:
                continue
            silent_for = now - state.last_seen
            if silent_for > self.stale_after:
                state.active.add(AlertKind.LOST_CONTACT)
                alerts.append(
                    Alert(
                        asset_id,
                        AlertKind.LOST_CONTACT,
                        Severity.CRITICAL,
                        f"no telemetry for {silent_for.total_seconds():.0f}s",
                        now,
                    )
                )
        return alerts

    def seed(self, asset_id: str, last_seq: int, last_seen: datetime) -> None:
        """Restore per-asset position after a restart so we don't misreport gaps."""
        self._assets[asset_id] = _AssetState(last_seq=last_seq, last_seen=last_seen)

    @staticmethod
    def _edge(
        state: _AssetState,
        alerts: list[Alert],
        t: Telemetry,
        kind: AlertKind,
        condition: bool,
        severity: Severity,
        message: str,
    ) -> None:
        if condition and kind not in state.active:
            state.active.add(kind)
            alerts.append(Alert(t.asset_id, kind, severity, message, t.ts))
        elif not condition:
            state.active.discard(kind)
