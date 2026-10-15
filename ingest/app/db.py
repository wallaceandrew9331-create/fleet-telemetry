from __future__ import annotations

from datetime import datetime

import asyncpg

from .models import TELEMETRY_COLUMNS, Alert, Telemetry

# Latest-state writes take whole columns as arrays, so one statement covers the
# batch instead of one Bind/Execute per asset. Argument order is
# TELEMETRY_COLUMNS: ts, asset_id, seq, lat, lon, alt_km, battery_pct, temp_c,
# signal_db, status.
_UNNEST = """unnest($1::timestamptz[], $2::text[], $3::bigint[], $4::float8[], $5::float8[], $6::float8[],
                    $7::float8[], $8::float8[], $9::float8[], $10::text[])
         AS u(ts, asset_id, seq, lat, lon, alt_km, battery_pct, temp_c, signal_db, status)"""

# Known assets: a plain UPDATE joined to the batch. Measured ~4x cheaper than
# INSERT ... ON CONFLICT DO UPDATE for 5,000 rows, which pays for a speculative
# insert attempt on every row before falling through to the update.
UPDATE_ASSETS = f"""
UPDATE assets a SET
    last_seen = u.ts, last_seq = u.seq, lat = u.lat, lon = u.lon, alt_km = u.alt_km,
    battery_pct = u.battery_pct, temp_c = u.temp_c, signal_db = u.signal_db, status = u.status
FROM {_UNNEST}
WHERE a.asset_id = u.asset_id AND a.last_seen <= u.ts
"""

# First sighting of an asset (in this process). ON CONFLICT keeps it correct if
# another replica, or an earlier run, inserted the row first.
UPSERT_ASSETS = f"""
INSERT INTO assets (asset_id, last_seen, last_seq, lat, lon, alt_km, battery_pct, temp_c, signal_db, status)
SELECT u.asset_id, u.ts, u.seq, u.lat, u.lon, u.alt_km, u.battery_pct, u.temp_c, u.signal_db, u.status
FROM {_UNNEST}
ON CONFLICT (asset_id) DO UPDATE SET
    last_seen = EXCLUDED.last_seen, last_seq = EXCLUDED.last_seq,
    lat = EXCLUDED.lat, lon = EXCLUDED.lon, alt_km = EXCLUDED.alt_km,
    battery_pct = EXCLUDED.battery_pct, temp_c = EXCLUDED.temp_c,
    signal_db = EXCLUDED.signal_db, status = EXCLUDED.status
WHERE assets.last_seen <= EXCLUDED.last_seen
"""


def latest_per_asset(batch: list[Telemetry]) -> list[Telemetry]:
    latest: dict[str, Telemetry] = {}
    for t in batch:
        cur = latest.get(t.asset_id)
        if cur is None or t.ts >= cur.ts:
            latest[t.asset_id] = t
    return list(latest.values())


def split_known(rows: list[Telemetry], known: set[str]) -> tuple[list[Telemetry], list[Telemetry]]:
    """Partition into (assets already in the table, assets not yet seen)."""
    existing, new = [], []
    for t in rows:
        (existing if t.asset_id in known else new).append(t)
    return existing, new


def as_columns(rows: list[Telemetry]) -> list[list]:
    """Transpose rows into one list per column, the shape unnest() takes."""
    return [list(col) for col in zip(*(t.as_row() for t in rows), strict=True)]


class Repository:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        # Asset rows are never deleted, so once an id is in the table it stays
        # there and its writes can take the cheaper UPDATE path.
        self.known_assets: set[str] = set()

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
        existing, new = split_known(latest_per_asset(batch), self.known_assets)

        async with self.pool.acquire() as conn, conn.transaction():
            if batch:
                await conn.copy_records_to_table(
                    "telemetry", records=[t.as_row() for t in batch], columns=TELEMETRY_COLUMNS
                )
            if existing:
                await conn.execute(UPDATE_ASSETS, *as_columns(existing))
            if new:
                await conn.execute(UPSERT_ASSETS, *as_columns(new))
            if alerts:
                await conn.executemany(
                    "INSERT INTO alerts (ts, asset_id, kind, severity, message) VALUES ($1, $2, $3, $4, $5)",
                    [(a.ts, a.asset_id, a.kind.value, a.severity.value, a.message) for a in alerts],
                )
        # Only after commit: a rolled-back insert must be retried as an insert.
        self.known_assets.update(t.asset_id for t in new)

    async def insert_alerts(self, alerts: list[Alert]) -> None:
        await self.write_batch([], alerts)

    async def load_assets(self) -> list[asyncpg.Record]:
        rows = await self.pool.fetch("SELECT * FROM assets")
        self.known_assets.update(r["asset_id"] for r in rows)
        return rows

    async def asset_history(self, asset_id: str, since: datetime) -> list[asyncpg.Record]:
        return await self.pool.fetch(
            """SELECT ts, battery_pct, temp_c, signal_db, alt_km, status
               FROM telemetry WHERE asset_id = $1 AND ts >= $2 ORDER BY ts""",
            asset_id,
            since,
        )

    async def recent_alerts(self, limit: int) -> list[asyncpg.Record]:
        return await self.pool.fetch("SELECT * FROM alerts ORDER BY ts DESC LIMIT $1", limit)
