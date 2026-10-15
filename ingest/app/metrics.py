from prometheus_client import Counter, Gauge, Histogram

MESSAGES = Counter("ingest_messages_total", "Telemetry messages consumed", ["result"])
ALERTS = Counter("ingest_alerts_total", "Alerts raised", ["kind", "severity"])
BATCH_SIZE = Histogram(
    "ingest_batch_size", "Records per consumer batch", buckets=(1, 10, 50, 100, 500, 1000, 2500, 5000)
)
DB_WRITE_SECONDS = Histogram(
    "ingest_db_write_seconds",
    "Time to persist one batch",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
END_TO_END_SECONDS = Histogram(
    "ingest_end_to_end_seconds",
    "Asset timestamp to persisted, per message",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)
WS_CLIENTS = Gauge("ingest_ws_clients", "Connected dashboard clients")
ASSETS_TRACKED = Gauge("ingest_assets_tracked", "Distinct assets seen")
CONSUMER_LAG_SECONDS = Gauge("ingest_consumer_lag_seconds", "Wall time minus newest processed event time")
