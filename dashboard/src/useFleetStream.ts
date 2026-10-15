import { useEffect, useReducer, useState } from "react";
import { applyMessage, emptyFleet } from "./fleet";
import type { StreamMessage } from "./types";

export type ConnectionState = "connecting" | "live" | "reconnecting";

// Subscribes to the ingest WebSocket and reconnects with capped backoff. Every
// (re)connect starts with a full snapshot, so no client-side replay is needed.
export function useFleetStream() {
  const [fleet, dispatch] = useReducer(applyMessage, emptyFleet);
  const [conn, setConn] = useState<ConnectionState>("connecting");

  useEffect(() => {
    let ws: WebSocket | undefined;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let attempt = 0;
    let closed = false;

    const connect = () => {
      const proto = location.protocol === "https:" ? "wss" : "ws";
      ws = new WebSocket(`${proto}://${location.host}/ws`);
      ws.onopen = () => {
        attempt = 0;
        setConn("live");
      };
      ws.onmessage = (ev) => dispatch(JSON.parse(ev.data) as StreamMessage);
      ws.onclose = () => {
        if (closed) return;
        setConn("reconnecting");
        retry = setTimeout(connect, Math.min(1000 * 2 ** attempt++, 15_000));
      };
    };
    connect();

    return () => {
      closed = true;
      clearTimeout(retry);
      ws?.close();
    };
  }, []);

  return { fleet, conn };
}

// Re-render periodically so "stale" status appears even when no messages arrive.
export function useNow(intervalMs = 1000): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
  return now;
}
