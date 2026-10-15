"""Latest-state write strategies against the real TimescaleDB.

Run on the compose network with the simulator and ingest stopped:

  docker run --rm --network fleetperf_default -v "$PWD/perf:/b" python:3.12-slim \
    sh -c "pip install -q asyncpg==0.30.0 && python /b/bench_upsert.py"
"""

import asyncio
import random
import statistics
import time
from datetime import UTC, datetime, timedelta

import asyncpg

COLS = ("ts", "asset_id", "seq", "lat", "lon", "alt_km", "battery_pct", "temp_c", "signal_db", "status")
SET = """last_seen = EXCLUDED.last_seen, last_seq = EXCLUDED.last_seq, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
    alt_km = EXCLUDED.alt_km, battery_pct = EXCLUDED.battery_pct, temp_c = EXCLUDED.temp_c,
    signal_db = EXCLUDED.signal_db, status = EXCLUDED.status"""
UNNEST = """unnest($1::timestamptz[], $2::text[], $3::bigint[], $4::float8[], $5::float8[], $6::float8[],
    $7::float8[], $8::float8[], $9::float8[], $10::text[])
    AS u(ts, asset_id, seq, lat, lon, alt_km, battery_pct, temp_c, signal_db, status)"""
INSERT = "INSERT INTO assets (asset_id, last_seen, last_seq, lat, lon, alt_km, battery_pct, temp_c, signal_db, status)"

# The original per-row statement (one Bind/Execute per asset via executemany).
EXECUTEMANY = f"""{INSERT} VALUES ($2, $1, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (asset_id) DO UPDATE SET {SET} WHERE assets.last_seen <= EXCLUDED.last_seen"""
UNNEST_UPSERT = f"""{INSERT}
SELECT u.asset_id, u.ts, u.seq, u.lat, u.lon, u.alt_km, u.battery_pct, u.temp_c, u.signal_db, u.status FROM {UNNEST}
ON CONFLICT (asset_id) DO UPDATE SET {SET} WHERE assets.last_seen <= EXCLUDED.last_seen"""
UPDATE_FROM = f"""UPDATE assets a SET last_seen = u.ts, last_seq = u.seq, lat = u.lat, lon = u.lon,
    alt_km = u.alt_km, battery_pct = u.battery_pct, temp_c = u.temp_c, signal_db = u.signal_db, status = u.status
FROM {UNNEST} WHERE a.asset_id = u.asset_id AND a.last_seen <= u.ts"""


def rows(n: int, t: datetime) -> list[tuple]:
    return [
        (t, f"SAT-{i:05d}", 1, random.uniform(-90, 90), random.uniform(-180, 180), 550.0, 80.0, 20.0, -90.0, "nominal")
        for i in range(n)
    ]


async def main() -> None:
    conn = await asyncpg.connect("postgresql://fleet:fleet@postgres:5432/fleet")
    t = datetime.now(UTC) + timedelta(days=1)  # newer than anything already in the table
    for n in (500, 5000):
        await conn.execute(
            f"{INSERT} SELECT u.asset_id, u.ts, u.seq, u.lat, u.lon, u.alt_km, u.battery_pct, "
            f"u.temp_c, u.signal_db, u.status FROM {UNNEST} ON CONFLICT DO NOTHING",
            *[list(c) for c in zip(*rows(n, t), strict=True)],
        )
        timings: dict[str, list[float]] = {
            "copy (telemetry)": [],
            "executemany upsert": [],
            "unnest upsert": [],
            "update from unnest": [],
        }
        for _ in range(15):
            for name in timings:
                t += timedelta(seconds=1)
                r = rows(n, t)
                cols = [list(c) for c in zip(*r, strict=True)]
                async with conn.transaction():
                    start = time.perf_counter()
                    if name.startswith("copy"):
                        await conn.copy_records_to_table("telemetry", records=r, columns=COLS)
                    elif name.startswith("executemany"):
                        await conn.executemany(EXECUTEMANY, r)
                    elif name.startswith("unnest"):
                        await conn.execute(UNNEST_UPSERT, *cols)
                    else:
                        await conn.execute(UPDATE_FROM, *cols)
                    timings[name].append(time.perf_counter() - start)
        for name, v in timings.items():
            med = statistics.median(v[3:])  # drop warm-up
            print(f"n={n:5d}  {name:20s} median {1000 * med:7.2f} ms   {1e6 * med / n:5.1f} us/row")
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
