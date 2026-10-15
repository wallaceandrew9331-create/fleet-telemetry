import { useEffect, useState } from "react";
import type { Asset, HistoryPoint } from "../types";

const REFRESH_MS = 5000;

interface Props {
  asset: Asset;
  onClose: () => void;
}

export function AssetDetail({ asset, onClose }: Props) {
  const [history, setHistory] = useState<HistoryPoint[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const res = await fetch(`/api/assets/${encodeURIComponent(asset.asset_id)}/history?minutes=15`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = (await res.json()) as HistoryPoint[];
        if (!cancelled) {
          setHistory(data);
          setError(null);
        }
      } catch (e) {
        if (!cancelled) setError(String(e));
      }
    };
    load();
    const id = setInterval(load, REFRESH_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [asset.asset_id]);

  return (
    <div className="panel detail">
      <h2>
        <span className="mono">{asset.asset_id}</span>
        <button onClick={onClose} aria-label="Close detail">
          ×
        </button>
      </h2>
      <dl>
        <dt>Position</dt>
        <dd>
          {asset.lat.toFixed(2)}, {asset.lon.toFixed(2)} @ {asset.alt_km.toFixed(1)} km
        </dd>
        <dt>Status</dt>
        <dd>{asset.status}</dd>
        <dt>Sequence</dt>
        <dd>{asset.seq}</dd>
      </dl>
      {error && <p className="error">History unavailable: {error}</p>}
      <Sparkline label="Battery %" values={history.map((h) => h.battery_pct)} min={0} max={100} />
      <Sparkline label="Temp °C" values={history.map((h) => h.temp_c)} />
      <Sparkline label="Signal dB" values={history.map((h) => h.signal_db)} />
    </div>
  );
}

function Sparkline({ label, values, min, max }: { label: string; values: number[]; min?: number; max?: number }) {
  const w = 280;
  const h = 48;
  const lo = min ?? Math.min(...values);
  const hi = max ?? Math.max(...values);
  const span = hi - lo || 1;
  const points = values
    .map((v, i) => `${(i / Math.max(1, values.length - 1)) * w},${h - ((v - lo) / span) * h}`)
    .join(" ");
  const last = values.at(-1);
  return (
    <figure className="spark">
      <figcaption>
        {label} <strong>{last === undefined ? "–" : last.toFixed(1)}</strong>
      </figcaption>
      <svg viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none">
        {values.length > 1 && <polyline points={points} />}
      </svg>
    </figure>
  );
}
