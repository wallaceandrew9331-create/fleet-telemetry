from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class Telemetry(BaseModel):
    """Wire format produced by the simulator (simulator/asset.go)."""

    asset_id: str = Field(min_length=1, max_length=64)
    seq: int = Field(ge=0)
    ts: datetime
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    alt_km: float
    battery_pct: float = Field(ge=0, le=100)
    temp_c: float
    signal_db: float
    status: str

    def as_row(self) -> tuple:
        return (
            self.ts,
            self.asset_id,
            self.seq,
            self.lat,
            self.lon,
            self.alt_km,
            self.battery_pct,
            self.temp_c,
            self.signal_db,
            self.status,
        )


TELEMETRY_COLUMNS = (
    "ts",
    "asset_id",
    "seq",
    "lat",
    "lon",
    "alt_km",
    "battery_pct",
    "temp_c",
    "signal_db",
    "status",
)


class Severity(StrEnum):
    WARNING = "warning"
    CRITICAL = "critical"


class AlertKind(StrEnum):
    LOW_BATTERY = "low_battery"
    OVER_TEMP = "over_temp"
    SAFE_MODE = "safe_mode"
    SEQ_GAP = "seq_gap"
    LOST_CONTACT = "lost_contact"
    RESTART = "restart"


@dataclass(frozen=True)
class Alert:
    asset_id: str
    kind: AlertKind
    severity: Severity
    message: str
    ts: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_json(self) -> dict:
        return {
            "asset_id": self.asset_id,
            "kind": self.kind.value,
            "severity": self.severity.value,
            "message": self.message,
            "ts": self.ts.isoformat(),
        }
