"""Kafka -> anomaly detection -> Postgres -> dashboard hub.

Delivery is at-least-once: offsets are committed only after a batch is
durably written. If the database is down, the consumer retries the same batch
and stops pulling new records, which pushes backpressure into Kafka instead of
into this process's memory.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime

import asyncpg
from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from . import metrics
from .anomaly import AnomalyDetector, StreamClock
from .config import Settings
from .db import Repository
from .hub import FleetHub
from .models import Alert, Telemetry

log = logging.getLogger("ingest.consumer")


def parse_batch(values: list[bytes]) -> tuple[list[Telemetry], int]:
    batch, rejected = [], 0
    for v in values:
        try:
            batch.append(Telemetry.model_validate_json(v))
        except ValidationError:
            rejected += 1
    return batch, rejected


def record_alert_metrics(alerts: list[Alert]) -> None:
    for a in alerts:
        metrics.ALERTS.labels(a.kind.value, a.severity.value).inc()


async def write_with_retry(repo: Repository, batch: list[Telemetry], alerts: list[Alert]) -> None:
    delay = 0.5
    while True:
        try:
            with metrics.DB_WRITE_SECONDS.time():
                await repo.write_batch(batch, alerts)
            return
        except (OSError, asyncpg.PostgresError) as exc:
            log.warning("db write failed, retrying in %.1fs: %s", delay, exc)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10)


async def run_consumer(
    settings: Settings, repo: Repository, hub: FleetHub, detector: AnomalyDetector, clock: StreamClock
) -> None:
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_brokers,
        group_id=settings.kafka_group,
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )
    await start_with_retry(consumer)
    log.info("consuming %s from %s", settings.kafka_topic, settings.kafka_brokers)
    try:
        while True:
            by_partition = await consumer.getmany(
                timeout_ms=settings.batch_timeout_ms, max_records=settings.batch_max_records
            )
            values = [r.value for records in by_partition.values() for r in records]
            if not values:
                continue

            batch, rejected = parse_batch(values)
            metrics.MESSAGES.labels("rejected").inc(rejected)
            metrics.BATCH_SIZE.observe(len(values))

            alerts = [a for t in batch for a in detector.observe(t)]
            await write_with_retry(repo, batch, alerts)
            await consumer.commit()

            now = datetime.now(UTC)
            for t in batch:
                metrics.END_TO_END_SECONDS.observe((now - t.ts).total_seconds())
            if batch:
                clock.advance(max(t.ts for t in batch), now)
            metrics.MESSAGES.labels("ok").inc(len(batch))
            record_alert_metrics(alerts)
            hub.apply(batch, alerts)
            metrics.ASSETS_TRACKED.set(len(hub.assets))
    finally:
        await consumer.stop()


async def start_with_retry(consumer: AIOKafkaConsumer, attempts: int = 30) -> None:
    for i in range(attempts):
        try:
            await consumer.start()
            return
        except Exception as exc:  # aiokafka raises several types while the broker boots
            log.warning("kafka not ready (%s), attempt %d/%d", exc, i + 1, attempts)
            await asyncio.sleep(2)
    raise RuntimeError("could not connect to Kafka")


async def run_stale_checker(
    settings: Settings, repo: Repository, hub: FleetHub, detector: AnomalyDetector, clock: StreamClock
) -> None:
    while True:
        await asyncio.sleep(settings.expected_interval.total_seconds())
        wall = datetime.now(UTC)
        metrics.CONSUMER_LAG_SECONDS.set(clock.lag(wall).total_seconds())
        alerts = detector.check_stale(clock.now(wall))
        if alerts:
            await write_with_retry(repo, [], alerts)
            record_alert_metrics(alerts)
            hub.apply([], alerts)


async def run_broadcaster(settings: Settings, hub: FleetHub) -> None:
    while True:
        started = time.perf_counter()
        delta = hub.take_delta()
        if delta is not None:
            hub.broadcast(delta)
        metrics.WS_CLIENTS.set(hub.client_count)
        await asyncio.sleep(max(0.0, settings.broadcast_interval_s - (time.perf_counter() - started)))
