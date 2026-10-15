import { memo } from "react";
import { healthOf } from "../fleet";
import type { Asset } from "../types";

const W = 720;
const H = 360;

// Equirectangular projection: longitude/latitude map linearly onto x/y.
const x = (lon: number) => ((lon + 180) / 360) * W;
const y = (lat: number) => ((90 - lat) / 180) * H;

interface Props {
  assets: Asset[];
  now: number;
  selected: string | null;
  onSelect: (id: string) => void;
}

function FleetMapImpl({ assets, now, selected, onSelect }: Props) {
  return (
    <svg className="map" viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Asset positions">
      {[-60, -30, 0, 30, 60].map((lat) => (
        <line key={`lat${lat}`} className="grid" x1={0} x2={W} y1={y(lat)} y2={y(lat)} />
      ))}
      {[-120, -60, 0, 60, 120].map((lon) => (
        <line key={`lon${lon}`} className="grid" x1={x(lon)} x2={x(lon)} y1={0} y2={H} />
      ))}
      {assets.map((a) => (
        <circle
          key={a.asset_id}
          className={`dot ${healthOf(a, now)}${a.asset_id === selected ? " selected" : ""}`}
          cx={x(a.lon)}
          cy={y(a.lat)}
          r={a.asset_id === selected ? 5 : 2.2}
          onClick={() => onSelect(a.asset_id)}
        >
          <title>{a.asset_id}</title>
        </circle>
      ))}
    </svg>
  );
}

export const FleetMap = memo(FleetMapImpl);
