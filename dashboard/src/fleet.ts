import type { Alert, Asset, Health, StreamMessage } from "./types";

export const MAX_ALERTS = 200;
export const STALE_AFTER_MS = 10_000;

export interface FleetState {
  assets: Record<string, Asset>;
  alerts: Alert[];
}

export const emptyFleet: FleetState = { assets: {}, alerts: [] };

export function applyMessage(state: FleetState, msg: StreamMessage): FleetState {
  if (msg.type === "snapshot") {
    const assets: Record<string, Asset> = {};
    for (const a of msg.assets) assets[a.asset_id] = a;
    return { assets, alerts: msg.alerts.slice(0, MAX_ALERTS) };
  }
  if (msg.assets.length === 0 && msg.alerts.length === 0) return state;

  const assets = { ...state.assets };
  for (const a of msg.assets) {
    const prev = assets[a.asset_id];
    if (!prev || prev.seq < a.seq) assets[a.asset_id] = a;
  }
  // Server sends alerts oldest-first within a delta; the feed shows newest first.
  const alerts = [...msg.alerts].reverse().concat(state.alerts).slice(0, MAX_ALERTS);
  return { assets, alerts };
}

export function healthOf(a: Asset, now: number): Health {
  if (now - Date.parse(a.ts) > STALE_AFTER_MS) return "stale";
  if (a.status === "safe_mode" || a.battery_pct < 10 || a.temp_c > 70) return "critical";
  if (a.battery_pct < 20 || a.temp_c > 55) return "warning";
  return "ok";
}

const healthRank: Record<Health, number> = { critical: 0, stale: 1, warning: 2, ok: 3 };

export function worstFirst(assets: Asset[], now: number): Asset[] {
  return [...assets].sort(
    (x, y) =>
      healthRank[healthOf(x, now)] - healthRank[healthOf(y, now)] || x.asset_id.localeCompare(y.asset_id),
  );
}

export function countByHealth(assets: Asset[], now: number): Record<Health, number> {
  const counts: Record<Health, number> = { ok: 0, warning: 0, critical: 0, stale: 0 };
  for (const a of assets) counts[healthOf(a, now)]++;
  return counts;
}
