from __future__ import annotations

from datetime import datetime

import asyncpg

from .models import TELEMETRY_COLUMNS, Alert, Telemetry

UPSERT_ASSET = """
INSERT INTO assets (asset_id, last_seen, last_seq, lat, lon, alt_km, battery_pct, temp_c, signal_db, status)
VALUES ($2, $1, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (asset_id) DO UPDATE SET
    last_seen = EXCLUDED.last_seen, last_seq = EXCLUDED.last_seq,
    lat = EXCLUDED.lat, lon = EXCLUDED.lon, alt_km = EXCLUDED.alt_km,
    battery_pct = EXCLUDED.battery_pct, temp_c = EXCLUDED.temp_c,
    signal_db = EXCLUDED.signal_db, status = EXCLUDED.status
WHERE assets.last_seen <= EXCLUDED.last_seen
"""


class Repository:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str) -> Repository:
        return cls(await asyncpg.create_pool(dsn, min_size=2, max_size=10))

    async def close(self) -> None:
        await self.pool.close()

    async def write_batch(self, batch: list[Telemetry], alerts: list[Alert]) -> None:
        """Persist one consumer batch in a single transaction.

        Raw rows go through COPY, which is far cheaper than row-by-row INSERT
        at this volume. The asset table only needs the newest row per asset.
        """
        latest: dict[str, Telemetry] = {}
        for t in batch:
            cur = latest.get(t.asset_id)
            if cur is None or t.ts >= cur.ts:
                latest[t.asset_id] = t

        async with self.pool.acquire() as conn, conn.transaction():
            if batch:
                await conn.copy_records_to_table(
                    "telemetry", records=[t.as_row() for t in batch], columns=TELEMETRY_COLUMNS
                )
                await conn.executemany(UPSERT_ASSET, [t.as_row() for t in latest.values()])
            if alerts:
                await conn.executemany(
                    "INSERT INTO alerts (ts, asset_id, kind, severity, message) VALUES ($1, $2, $3, $4, $5)",
                    [(a.ts, a.asset_id, a.kind.value, a.severity.value, a.message) for a in alerts],
                )

    async def insert_alerts(self, alerts: list[Alert]) -> None:
        await self.write_batch([], alerts)

    async def load_assets(self) -> list[asyncpg.Record]:
        return await self.pool.fetch("SELECT * FROM assets")

    async def asset_history(self, asset_id: str, since: datetime) -> list[asyncpg.Record]:
        return await self.pool.fetch(
            """SELECT ts, battery_pct, temp_c, signal_db, alt_km, status
               FROM telemetry WHERE asset_id = $1 AND ts >= $2 ORDER BY ts""",
            asset_id,
            since,
        )

    async def recent_alerts(self, limit: int) -> list[asyncpg.Record]:
        return await self.pool.fetch("SELECT * FROM alerts ORDER BY ts DESC LIMIT $1", limit)
