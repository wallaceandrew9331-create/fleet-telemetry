import os
from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class Settings:
    kafka_brokers: str = os.getenv("KAFKA_BROKERS", "localhost:19092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "telemetry")
    kafka_group: str = os.getenv("KAFKA_GROUP", "ingest")
    database_url: str = os.getenv("DATABASE_URL", "postgresql://fleet:fleet@localhost:5432/fleet")
    expected_interval: timedelta = timedelta(milliseconds=int(os.getenv("EXPECTED_INTERVAL_MS", "1000")))
    batch_max_records: int = int(os.getenv("BATCH_MAX_RECORDS", "5000"))
    batch_timeout_ms: int = int(os.getenv("BATCH_TIMEOUT_MS", "250"))
    broadcast_interval_s: float = float(os.getenv("BROADCAST_INTERVAL_S", "1.0"))


settings = Settings()
