"""In-memory fleet view and WebSocket fan-out.

The consumer may process thousands of messages per second; browsers don't
need that. The hub coalesces updates per asset and pushes one delta per
broadcast interval, so dashboard cost scales with fleet size, not message rate.
"""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime

from .models import Alert, Telemetry

RECENT_ALERTS = 100
CLIENT_QUEUE_DEPTH = 8


class FleetHub:
    def __init__(self) -> None:
        self.assets: dict[str, dict] = {}
        self._last_ts: dict[str, datetime] = {}
        self.recent_alerts: deque[dict] = deque(maxlen=RECENT_ALERTS)
        self._dirty: set[str] = set()
        self._pending_alerts: list[dict] = []
        self._clients: set[asyncio.Queue] = set()
        self.dropped_clients = 0

    def apply(self, batch: list[Telemetry], alerts: list[Alert]) -> None:
        # Recency is judged by timestamp, not seq: seq resets when an asset reboots.
        for t in batch:
            prev_ts = self._last_ts.get(t.asset_id)
            if prev_ts is not None and t.ts < prev_ts:
                continue
            self._last_ts[t.asset_id] = t.ts
            self.assets[t.asset_id] = t.model_dump(mode="json")
            self._dirty.add(t.asset_id)
        for a in alerts:
            j = a.to_json()
            self.recent_alerts.appendleft(j)
            self._pending_alerts.append(j)

    def snapshot(self) -> dict:
        return {
            "type": "snapshot",
            "assets": list(self.assets.values()),
            "alerts": list(self.recent_alerts),
        }

    def take_delta(self) -> dict | None:
        if not self._dirty and not self._pending_alerts:
            return None
        delta = {
            "type": "delta",
            "assets": [self.assets[a] for a in self._dirty],
            "alerts": self._pending_alerts,
        }
        self._dirty = set()
        self._pending_alerts = []
        return delta

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=CLIENT_QUEUE_DEPTH)
        self._clients.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._clients.discard(q)

    def broadcast(self, message: dict) -> None:
        # A client that can't keep up is disconnected rather than allowed to
        # buffer unbounded memory; it will reconnect and get a fresh snapshot.
        # None in the queue tells the websocket loop to close.
        for q in list(self._clients):
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                self._clients.discard(q)
                self.dropped_clients += 1
                while not q.empty():
                    q.get_nowait()
                q.put_nowait(None)

    @property
    def client_count(self) -> int:
        return len(self._clients)
