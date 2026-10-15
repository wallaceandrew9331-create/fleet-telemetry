// Mirrors ingest/app/models.py.
export interface Asset {
  asset_id: string;
  seq: number;
  ts: string;
  lat: number;
  lon: number;
  alt_km: number;
  battery_pct: number;
  temp_c: number;
  signal_db: number;
  status: string;
}

export interface Alert {
  asset_id: string;
  kind: string;
  severity: "warning" | "critical";
  message: string;
  ts: string;
}

export type StreamMessage =
  | { type: "snapshot"; assets: Asset[]; alerts: Alert[] }
  | { type: "delta"; assets: Asset[]; alerts: Alert[] };

export type Health = "ok" | "warning" | "critical" | "stale";

export interface HistoryPoint {
  ts: string;
  battery_pct: number;
  temp_c: number;
  signal_db: number;
  alt_km: number;
  status: string;
}
