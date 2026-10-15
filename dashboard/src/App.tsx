import { useMemo, useState } from "react";
import { AlertFeed } from "./components/AlertFeed";
import { AssetDetail } from "./components/AssetDetail";
import { AssetTable } from "./components/AssetTable";
import { FleetMap } from "./components/FleetMap";
import { countByHealth } from "./fleet";
import type { Health } from "./types";
import { useFleetStream, useNow } from "./useFleetStream";

const HEALTH_ORDER: Health[] = ["ok", "warning", "critical", "stale"];

export function App() {
  const { fleet, conn } = useFleetStream();
  const now = useNow();
  const [selected, setSelected] = useState<string | null>(null);

  const assets = useMemo(() => Object.values(fleet.assets), [fleet.assets]);
  const counts = countByHealth(assets, now);
  const selectedAsset = selected ? fleet.assets[selected] : undefined;

  return (
    <div className="app">
      <header>
        <h1>Fleet Telemetry</h1>
        <span className={`conn ${conn}`}>{conn}</span>
      </header>

      <section className="stats">
        <div className="stat">
          <span className="value">{assets.length}</span>
          <span className="label">assets</span>
        </div>
        {HEALTH_ORDER.map((h) => (
          <div key={h} className={`stat ${h}`}>
            <span className="value">{counts[h]}</span>
            <span className="label">{h}</span>
          </div>
        ))}
      </section>

      <main>
        <div className="col">
          <div className="panel">
            <FleetMap assets={assets} now={now} selected={selected} onSelect={setSelected} />
          </div>
          <AssetTable assets={assets} now={now} selected={selected} onSelect={setSelected} />
        </div>
        <div className="col side">
          {selectedAsset && <AssetDetail asset={selectedAsset} onClose={() => setSelected(null)} />}
          <AlertFeed alerts={fleet.alerts} onSelect={setSelected} />
        </div>
      </main>
    </div>
  );
}
