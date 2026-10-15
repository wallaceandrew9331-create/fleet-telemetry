CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Latest known state per asset. Upserted by the ingest service on every batch,
-- so the dashboard never has to scan the telemetry hypertable for "current" values.
CREATE TABLE IF NOT EXISTS assets (
    asset_id     TEXT PRIMARY KEY,
    last_seen    TIMESTAMPTZ NOT NULL,
    last_seq     BIGINT      NOT NULL,
    lat          DOUBLE PRECISION NOT NULL,
    lon          DOUBLE PRECISION NOT NULL,
    alt_km       DOUBLE PRECISION NOT NULL,
    battery_pct  DOUBLE PRECISION NOT NULL,
    temp_c       DOUBLE PRECISION NOT NULL,
    signal_db    DOUBLE PRECISION NOT NULL,
    status       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS telemetry (
    ts           TIMESTAMPTZ NOT NULL,
    asset_id     TEXT        NOT NULL,
    seq          BIGINT      NOT NULL,
    lat          DOUBLE PRECISION NOT NULL,
    lon          DOUBLE PRECISION NOT NULL,
    alt_km       DOUBLE PRECISION NOT NULL,
    battery_pct  DOUBLE PRECISION NOT NULL,
    temp_c       DOUBLE PRECISION NOT NULL,
    signal_db    DOUBLE PRECISION NOT NULL,
    status       TEXT NOT NULL
);

SELECT create_hypertable('telemetry', by_range('ts', INTERVAL '1 hour'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS telemetry_asset_ts ON telemetry (asset_id, ts DESC);

-- Raw telemetry is high-volume; keep a day of it in this demo.
SELECT add_retention_policy('telemetry', INTERVAL '1 day', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS alerts (
    id           BIGSERIAL PRIMARY KEY,
    ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
    asset_id     TEXT NOT NULL,
    kind         TEXT NOT NULL,
    severity     TEXT NOT NULL,
    message      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS alerts_ts ON alerts (ts DESC);
