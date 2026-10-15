import type { Alert } from "../types";

interface Props {
  alerts: Alert[];
  onSelect: (id: string) => void;
}

export function AlertFeed({ alerts, onSelect }: Props) {
  return (
    <div className="panel">
      <h2>
        Alerts <span className="muted">{alerts.length}</span>
      </h2>
      {alerts.length === 0 ? (
        <p className="muted">No alerts yet.</p>
      ) : (
        <ul className="alerts">
          {alerts.map((a, i) => (
            <li key={`${a.ts}-${a.asset_id}-${a.kind}-${i}`} onClick={() => onSelect(a.asset_id)}>
              <span className={`pill ${a.severity}`}>{a.kind.replace("_", " ")}</span>
              <span className="mono">{a.asset_id}</span>
              <span className="muted">{a.message}</span>
              <time className="muted">{new Date(a.ts).toLocaleTimeString()}</time>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
