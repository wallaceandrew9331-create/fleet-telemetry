import { healthOf, worstFirst } from "../fleet";
import type { Asset } from "../types";

const ROWS = 50;

interface Props {
  assets: Asset[];
  now: number;
  selected: string | null;
  onSelect: (id: string) => void;
}

export function AssetTable({ assets, now, selected, onSelect }: Props) {
  const rows = worstFirst(assets, now).slice(0, ROWS);
  return (
    <div className="panel">
      <h2>
        Assets <span className="muted">worst {Math.min(ROWS, assets.length)} of {assets.length}</span>
      </h2>
      <table>
        <thead>
          <tr>
            <th>Asset</th>
            <th>Health</th>
            <th className="num">Battery</th>
            <th className="num">Temp</th>
            <th className="num">Signal</th>
            <th className="num">Age</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((a) => {
            const health = healthOf(a, now);
            return (
              <tr
                key={a.asset_id}
                className={a.asset_id === selected ? "selected" : undefined}
                onClick={() => onSelect(a.asset_id)}
              >
                <td className="mono">{a.asset_id}</td>
                <td>
                  <span className={`pill ${health}`}>{health}</span>
                </td>
                <td className="num">{a.battery_pct.toFixed(1)}%</td>
                <td className="num">{a.temp_c.toFixed(1)}°C</td>
                <td className="num">{a.signal_db.toFixed(0)} dB</td>
                <td className="num">{Math.max(0, (now - Date.parse(a.ts)) / 1000).toFixed(0)}s</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
