"""Windowed load-test snapshot: scrape ingest /metrics twice and diff.

usage: python perf/measure.py LABEL WINDOW_S [results.jsonl]

Rates and quantiles cover only the window, not the process lifetime. Quantiles
use the same linear interpolation within buckets as Prometheus
histogram_quantile(), so they are only as precise as the bucket layout.
"""

import json
import pathlib
import re
import subprocess
import sys
import time
import urllib.request
from collections import defaultdict

URL = "http://localhost:8000/metrics"
REPO = pathlib.Path(__file__).resolve().parent.parent
LINE = re.compile(r"^([a-zA-Z_:]+)(\{[^}]*\})?\s+([0-9.eE+\-]+|NaN|\+Inf)$")


def scrape() -> dict:
    text = urllib.request.urlopen(URL, timeout=10).read().decode()
    out = {}
    for ln in text.splitlines():
        m = LINE.match(ln)
        if m:
            out[(m.group(1), m.group(2) or "")] = float(m.group(3))
    return out


def hist(s0: dict, s1: dict, name: str):
    buckets = defaultdict(float)
    for (k, labels), v in s1.items():
        if k == name + "_bucket":
            le = re.search(r'le="([^"]+)"', labels).group(1)
            buckets[float("inf") if le == "+Inf" else float(le)] += v - s0.get((k, labels), 0.0)
    count = s1.get((name + "_count", ""), 0) - s0.get((name + "_count", ""), 0)
    total = s1.get((name + "_sum", ""), 0) - s0.get((name + "_sum", ""), 0)
    return sorted(buckets.items()), count, total


def quantile(buckets, q):
    total = buckets[-1][1]
    if total == 0:
        return None
    rank = q * total
    prev_le, prev_c = 0.0, 0.0
    for le, c in buckets:
        if c >= rank:
            if le == float("inf"):
                return prev_le  # Prometheus convention: clamp to the highest finite bucket
            return prev_le + (le - prev_le) * (rank - prev_c) / max(c - prev_c, 1e-9)
        prev_le, prev_c = le, c
    return prev_le


def sh(cmd: str) -> str:
    return subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, shell=True).stdout


def main() -> None:
    label, window = sys.argv[1], float(sys.argv[2])
    out_path = sys.argv[3] if len(sys.argv) > 3 else "results.jsonl"
    s0, t0 = scrape(), time.time()
    time.sleep(window)
    s1, t1 = scrape(), time.time()
    dt = t1 - t0

    res = {"label": label, "window_s": round(dt, 1)}
    ok = ("ingest_messages_total", '{result="ok"}')
    res["ok_msgs_per_s"] = round((s1.get(ok, 0) - s0.get(ok, 0)) / dt, 1)
    for name, key, scale in [
        ("ingest_end_to_end_seconds", "e2e_ms", 1000),
        ("ingest_db_write_seconds", "db_write_ms", 1000),
        ("ingest_batch_size", "batch", 1),
    ]:
        b, n, tot = hist(s0, s1, name)
        r = {"count": n, "mean": round(tot / n * scale, 2) if n else None}
        for q in (0.5, 0.95, 0.99):
            v = quantile(b, q) if b else None
            r[f"p{int(q * 100)}"] = round(v * scale, 2) if v is not None else None
        res[key] = r
    res["batches_per_s"] = round(res["batch"]["count"] / dt, 2)
    res["docker_stats"] = sh('docker stats --no-stream --format "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"').splitlines()
    res["lag"] = sh("docker compose exec -T redpanda rpk group describe ingest").strip()
    sim_logs = sh("docker compose logs --tail 400 simulator").splitlines()
    res["simulator"] = [ln for ln in sim_logs if "throughput" in ln][-3:]

    print(json.dumps(res, indent=2))
    with open(out_path, "a") as f:
        f.write(json.dumps(res) + "\n")


if __name__ == "__main__":
    main()
