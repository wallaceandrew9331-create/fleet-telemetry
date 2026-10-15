import { describe, expect, it } from "vitest";
import { applyMessage, countByHealth, emptyFleet, healthOf, worstFirst } from "./fleet";
import type { Alert, Asset } from "./types";

const NOW = Date.parse("2026-01-01T00:00:10Z");

function asset(id: string, overrides: Partial<Asset> = {}): Asset {
  return {
    asset_id: id,
    seq: 1,
    ts: "2026-01-01T00:00:09Z",
    lat: 0,
    lon: 0,
    alt_km: 550,
    battery_pct: 80,
    temp_c: 20,
    signal_db: -70,
    status: "nominal",
    ...overrides,
  };
}

function alert(id: string, kind = "seq_gap"): Alert {
  return { asset_id: id, kind, severity: "warning", message: "x", ts: "2026-01-01T00:00:09Z" };
}

describe("applyMessage", () => {
  it("replaces state on snapshot", () => {
    const s = applyMessage(emptyFleet, { type: "snapshot", assets: [asset("A")], alerts: [] });
    expect(Object.keys(s.assets)).toEqual(["A"]);
  });

  it("ignores out-of-order asset updates in a delta", () => {
    let s = applyMessage(emptyFleet, { type: "snapshot", assets: [asset("A", { seq: 5 })], alerts: [] });
    const older = asset("A", { seq: 3, battery_pct: 1, ts: "2026-01-01T00:00:07Z" });
    s = applyMessage(s, { type: "delta", assets: [older], alerts: [] });
    expect(s.assets.A.battery_pct).toBe(80);
  });

  it("accepts a rebooted asset whose seq reset", () => {
    let s = applyMessage(emptyFleet, { type: "snapshot", assets: [asset("A", { seq: 500 })], alerts: [] });
    const rebooted = asset("A", { seq: 1, ts: "2026-01-01T00:00:10Z" });
    s = applyMessage(s, { type: "delta", assets: [rebooted], alerts: [] });
    expect(s.assets.A.seq).toBe(1);
  });

  it("prepends new alerts newest-first", () => {
    let s = applyMessage(emptyFleet, { type: "snapshot", assets: [], alerts: [alert("OLD")] });
    s = applyMessage(s, { type: "delta", assets: [], alerts: [alert("A"), alert("B")] });
    expect(s.alerts.map((a) => a.asset_id)).toEqual(["B", "A", "OLD"]);
  });

  it("returns the same object for an empty delta", () => {
    const s = applyMessage(emptyFleet, { type: "snapshot", assets: [asset("A")], alerts: [] });
    expect(applyMessage(s, { type: "delta", assets: [], alerts: [] })).toBe(s);
  });
});

describe("health", () => {
  it("classifies assets", () => {
    expect(healthOf(asset("A"), NOW)).toBe("ok");
    expect(healthOf(asset("A", { battery_pct: 15 }), NOW)).toBe("warning");
    expect(healthOf(asset("A", { status: "safe_mode" }), NOW)).toBe("critical");
    expect(healthOf(asset("A", { ts: "2026-01-01T00:00:00Z" }), NOW + 5_000)).toBe("stale");
  });

  it("sorts worst first and counts", () => {
    const list = [asset("C"), asset("B", { temp_c: 60 }), asset("A", { battery_pct: 5 })];
    expect(worstFirst(list, NOW).map((a) => a.asset_id)).toEqual(["A", "B", "C"]);
    expect(countByHealth(list, NOW)).toEqual({ ok: 1, warning: 1, critical: 1, stale: 0 });
  });
});
