# Performance study

How far one ingest process scales, where it broke, and what changed it. All numbers below come from the runs recorded in [`perf/results.jsonl`](perf/results.jsonl). Each run uses a fresh database and a single 60-second measurement window, so treat differences under about 15% as noise (see [Caveats](#caveats)).

## Summary

| | Before | After |
|---|---|---|
| Single-consumer ceiling | ~15k msg/s | ~23.8k msg/s |
| 20k msg/s (10k assets @ 500 ms) | **falls behind**: consumes 14.7k/s, lag grows ~5k msg/s, end-to-end > 45 s | **keeps up**: lag 0.16 s, p99 end-to-end 0.81 s |
| 10k msg/s, p99 end-to-end | 238 ms | 175 ms |
| DB time per row at saturation | 44 µs | 21 µs |
| `lost_contact` alerts in a 20k msg/s run | 4.3M (vs 4.58M messages) | ~600, consistent with injected comms-loss faults |

Three changes, each found by measuring rather than guessing:

1. **Event-time staleness** ([`anomaly.py`](ingest/app/anomaly.py) `StreamClock`). A consumer that fell behind reported the whole fleet lost every 500 ms. This was a correctness bug that also used 27% of ingest CPU, but fixing it barely changed throughput.
2. **Set-based latest-state writes** ([`db.py`](ingest/app/db.py)). A per-row `executemany` upsert was replaced with a single `UPDATE … FROM unnest(...)`. This was the actual throughput limiter.
3. **Producer buffer** ([`simulator/main.go`](simulator/main.go)). The Go load generator collapsed at 40k msg/s, which hid the ingest ceiling.

## Setup

| | |
|---|---|
| Host | AMD Ryzen 9 5950X (16 cores / 32 threads), 128 GB RAM, Windows 11 Pro |
| Runtime | Docker Engine 29.2.1 on Docker Desktop (WSL2 VM: 32 vCPUs, 62.75 GiB) |
| Broker | Redpanda v24.2.7, `--mode dev-container --smp 1`. Topic auto-created with **1 partition** |
| Database | TimescaleDB 2.17.2 / Postgres 16, image defaults |
| Ingest | One Python 3.12 process (uvicorn, one asyncio loop), `BATCH_MAX_RECORDS=5000`, `BATCH_TIMEOUT_MS=250` |
| Producer | Go simulator: one goroutine per asset, franz-go, snappy, 20 ms linger, keyed by asset ID |

All five containers share one machine, so the simulator, broker, database and ingest compete for the same CPU.

## Method

[`perf/loadtest.sh`](perf/loadtest.sh) runs one load level:

1. `docker compose down -v`, then `up --build -d`, under a separate compose project (`fleetperf`), so every run starts with an empty database.
2. Wait 120 s for the stack to settle.
3. [`perf/measure.py`](perf/measure.py) scrapes `/metrics` at the start and end of a 60 s window. It computes rates and p50/p95/p99 from the **bucket deltas over that window**, using the same linear interpolation as Prometheus `histogram_quantile()`. It also takes one `docker stats --no-stream` sample (100% = one core), consumer lag from `rpk group describe ingest`, and the simulator's own `msgs_per_sec`/`failed_total` log lines.

Before measuring, I added finer histogram buckets. The end-to-end histogram previously started at 50 ms, while the baseline mean was ~18 ms, so p50 and p95 couldn't be resolved. The smallest buckets are now 5 ms (end-to-end) and 1 ms (DB write).

"End-to-end" means from the asset's timestamp to the batch being committed in Postgres. It includes the producer's 20 ms linger and the consumer's poll interval.

Profiling used py-spy 0.4.0 from a sidecar container that joins the ingest container's PID namespace ([`perf/profile.sh`](perf/profile.sh)), so the ingest image is unchanged. I took 200 Hz CPU profiles and also `--idle` profiles to see wall-clock time.

## Results

### Before

| Load | Produced msg/s | Consumed msg/s | e2e p50 / p95 / p99 (ms) | DB write mean / p99 (ms) | Mean batch | Ingest CPU | Postgres CPU | Kafka lag at end |
|---|---|---|---|---|---|---|---|---|
| 1k assets @ 1 s | 986 | 987 | 22 / 46 / 49 | 7.4 / 19.7 | 14 | 19% | 16% | 34 |
| 5k @ 1 s | 4,944 | 4,946 | 29 / 49 / 70 | 9.1 / 24.8 | 61 | 30% | 55% | 120 |
| 10k @ 1 s | 9,890 | 9,890 | 78 / 191 / 238 | 21.7 / 62.1 | 267 | 27% | 50% | 923 |
| 10k @ 500 ms | 19,773 | **14,748** | ≥ 45 s (backlog) | 205 / 492 | 4,658 | 51% | 74% | **999,278, growing** |

### After

| Load | Produced msg/s | Consumed msg/s | e2e p50 / p95 / p99 (ms) | DB write mean / p99 (ms) | Mean batch | Ingest CPU | Postgres CPU | Kafka lag at end |
|---|---|---|---|---|---|---|---|---|
| 1k assets @ 1 s | 987 | 989 | 19 / 44 / 49 | 4.4 / 9.8 | 14 | 15% | 11% | 0 |
| 5k @ 1 s | 4,941 | 4,944 | 22 / 47 / 50 | 7.2 / 23.5 | 60 | 28% | 23% | 110 |
| 10k @ 1 s | 9,924 | 9,905 | 40 / 93 / 175 | 11.6 / 38.4 | 152 | 45% | 32% | 451 |
| 10k @ 500 ms | 19,831 | **19,831** | 166 / 431 / 814 | 35.4 / 168 | 1,083 | 54% | 34% | 4,657 (0.16 s) |
| 10k @ 250 ms | 39,602 | **23,795** | ≥ 45 s (backlog) | 95.5 / 249 | 4,595 | 63% | 33% | 3.0M, growing |

How to read these tables:

- **At 1k and 5k nothing is under pressure.** End-to-end latency is dominated by the producer's 20 ms linger, and p95/p99 fall in the 25–50 ms bucket, so their exact values are interpolation. The DB-write improvement is real but doesn't show up in end-to-end latency here.
- **10k @ 1 s is where the old write path started to cost latency.** Mean batch DB time halved, and p95 end-to-end went from 191 ms to 93 ms.
- **At 20k the old pipeline couldn't keep up at all.** The new one keeps up with about 46% of wall-clock time to spare (see the profile below).
- **At 40k the new pipeline is saturated at ~23.8k msg/s.** That is the current single-consumer ceiling.
- **Batch size adapts to load.** The consumer takes whatever has arrived since its last poll. When it has spare capacity it runs many small batches; when it falls behind, batches hit the 5,000-record cap. A falling mean batch size at the same load means the consumer has more headroom.
- I ran a second 20k sample on the final code earlier in the session (before the simulator buffer change, which doesn't matter at that rate). It gave p50 224 / p99 902 ms, against 166 / 814 above, which is roughly the run-to-run variance.

## Finding 1: false-alert feedback loop (profiled, fixed, not the limiter)

CPU flame graph at 20k msg/s on the original code: [`perf/flamegraphs/before-20k.svg`](perf/flamegraphs/before-20k.svg).

| Inclusive CPU share | Before (20k) |
|---|---|
| `run_consumer` | 65.8% |
| ↳ `write_batch` | 32.7% (`executemany` 13.4%, COPY 5.6%) |
| ↳ `hub.apply` | 16.3% (`model_dump` 5.8%) |
| ↳ `parse_batch` (`model_validate_json`) | 13.5% |
| **`run_stale_checker`** | **26.8%** |

I expected the stale checker to be near zero. The cause:

- `check_stale` compared each asset's last processed timestamp against the **wall clock**.
- With the consumer 45 s behind, every asset's newest processed message was older than the 2.5 s threshold, so all 10,000 assets raised `lost_contact`.
- The next message from each asset cleared the flag, so 500 ms later they all fired again.
- `ingest_alerts_total{kind="lost_contact"}` read **4,305,271 against 4,583,232 messages**. Every one of those alerts was inserted into Postgres and pushed through the WebSocket hub, which added load to a consumer that was already behind.

**Fix: measure silence in event time.** `StreamClock` keeps a watermark: the newest event timestamp processed, clamped to wall time so one asset with a fast clock can't move it. Between batches the watermark advances with wall time, and `check_stale` compares against `watermark + (wall − time of last advance)`:

- Consumer current: ≈ wall clock, so behavior is unchanged.
- Consumer behind: the clock trails real time by the lag, so no false alerts.
- Feed stopped entirely: the clock keeps advancing, so a real outage still alerts.

Tests cover all three cases, plus a regression test that reproduces the storm ([`test_anomaly.py`](ingest/tests/test_anomaly.py)). Lag is now its own gauge, `ingest_consumer_lag_seconds`, so "we are behind" is a pipeline metric rather than 10,000 asset alerts.

**Result at 20k msg/s with only this fix:** `lost_contact` alerts dropped from 4.3M to 419. The remaining 419 are consistent with the simulator's injected comms-loss faults. Ingest CPU went from 51% to 44%. Throughput went from **14.7k to 15.0k msg/s, within noise**.

The biggest unexpected CPU consumer was not the throughput limiter. The `--idle` profile showed why: about 50% of wall-clock time was the event loop idle, waiting on I/O. The consumer loop is serial (poll → parse → write → commit), and per second it spent ~0.54 s awaiting Postgres plus ~0.44 s of Python CPU. Freeing CPU didn't help while the loop was waiting on the database.

## Finding 2: per-row latest-state upsert (the actual limiter)

Each batch writes raw rows with `COPY`, then upserts the newest row per asset into `assets`. At 20k msg/s a 5,000-record batch contains almost 5,000 distinct assets, so the upsert touches nearly as many rows as the COPY. To separate client overhead from server cost, I benchmarked the write strategies against the real TimescaleDB with the load stopped ([`perf/bench_upsert.py`](perf/bench_upsert.py), median of 12 runs after warm-up):

| 5,000 rows | Median | Per row |
|---|---|---|
| `COPY` into the `telemetry` hypertable | 25.5 ms | 5.1 µs |
| `executemany` `INSERT … ON CONFLICT DO UPDATE` (original) | 159.6 ms | 31.9 µs |
| single `INSERT … SELECT FROM unnest(...) ON CONFLICT DO UPDATE` | 117.6 ms | 23.5 µs |
| single `UPDATE assets … FROM unnest(...)` | **41.6 ms** | **8.3 µs** |

The original upsert cost about 6× as much per row as the COPY. Batching it into one statement only saved ~25%, so the cost was mostly server-side, not round-trips. `ON CONFLICT` makes a speculative insert attempt for every row before it falls through to the update. A plain `UPDATE` joined to the unnested arrays skips that, and was 3.8× faster.

**Fix** ([`db.py`](ingest/app/db.py)):

- Assets already in the table take the `UPDATE … FROM unnest` path.
- First-seen assets take a set-based `INSERT … ON CONFLICT` path, which stays correct if another replica or an earlier run inserted the row first.
- "Already in the table" is a set in the process, filled from the startup `load_assets()` and only after a commit succeeds. Asset rows are never deleted, so membership can't go stale.
- The `last_seen <= new ts` guard is kept on both paths, so at-least-once replays still can't rewind state.
- Pure helpers (`latest_per_asset`, `split_known`, `as_columns`) are unit-tested ([`test_db.py`](ingest/tests/test_db.py)).
- After a 20k msg/s run, `assets` held exactly 10,000 rows. The 99 that were more than 3 s old match the expected number of assets in simulated comms loss (~1% of the fleet).

**Result:** at 20k msg/s, mean DB write per batch went from 205 ms to 35 ms, Postgres CPU from 74% to 34%, and the consumer went from losing ground at ~5k msg/s to keeping up. At saturation the ceiling rose from ~15k to ~23.8k msg/s, and DB time per row fell from 44 µs to 21 µs.

Post-fix CPU profile at 20k msg/s: [`perf/flamegraphs/after-20k.svg`](perf/flamegraphs/after-20k.svg). The `--idle` profile shows the loop ~49% idle at that rate.

| Inclusive CPU share | After (20k) |
|---|---|
| `write_batch` | 30.4% (`execute` 14.9%, COPY 6.4%, `as_columns` 4.3%) |
| `parse_batch` (`model_validate_json`) | 16.8% |
| `hub.apply` | 14.4% (`model_dump` 6.9%) |
| `detector.observe` | 7.6% |
| aiokafka record unpacking | 7.3% |
| `run_stale_checker` | 2.8% |

## Finding 3: the load generator collapsed first

My first 40k msg/s run (10k assets @ 250 ms) showed ingest consuming only 199 msg/s. The cause was the **simulator**: 1,022% CPU while sending 165 msg/s, with no produce errors.

franz-go caps buffered records at 10,000 by default, which is one tick's worth at 10k assets. Once the buffer filled, every asset goroutine blocked in `Produce`, and each freed slot woke all of them. That thundering herd starved the client's own goroutines that drain the buffer.

Raising `MaxBufferedRecords` to 100,000 (~2.5 s of headroom at 40k msg/s, bounded memory) fixed it: 39.6k msg/s at ~92% of one core, and the bottleneck moved back to ingest. Without this fix, the 40k row would have measured the load generator, not the system.

## Remaining limits and next steps

In rough order of payoff:

1. **One partition, one serial consumer loop.** This is the ceiling at ~23.8k msg/s. At 40k the loop spends ~0.49 s of each second awaiting Postgres and runs at ~63% CPU; the two only overlap where aiokafka prefetches in the background. There are two options:
   - **Pipeline within one process:** parse batch N+1 while batch N is being written, committing offsets in order. This should get much of the ~2× that perfect overlap would give, without changing the architecture.
   - **More partitions plus consumer replicas:** the horizontal answer, but it's not free here. The anomaly detector and the WebSocket hub keep fleet state in each process. With N replicas each sees only its partitions, so the dashboard needs a fan-in (for example, replicas publish deltas to a compacted topic), and detector state must be re-seeded when partitions move during a rebalance.
2. **Python CPU per message.** After the fix, parse (17%), `hub.apply` (14%) and asyncpg encoding (~15%) dominate.
   - `hub.apply` serializes a dict for every message, but only the last state per asset per second is ever broadcast. Storing the model and serializing lazily in `take_delta` would remove most of that 14%.
   - Pydantic's `model_validate_json` is already the Rust fast path. Replacing it with orjson plus a hand-written check would save some CPU but give up schema validation.
3. **Alert inserts still use `executemany`.** That's fine at normal alert rates (a few per second), but a genuine fleet-wide event would insert alerts one by one, which is exactly the case Finding 1 hit. Switching to `COPY` is straightforward.
4. **`assets` fillfactor.** At the default fillfactor 100, 83% of updates were already HOT. In the benchmark, fillfactor 50 cut the `UPDATE` another ~20% (41 → 34 ms per 5k rows). I didn't apply it because `init.sql` only runs on new volumes, so it needs a migration.
5. **Dashboard not measured.** This study covers the ingest path only. Rendering 10k SVG dots in the browser is a separate roadmap item (canvas map) and wasn't profiled here.
6. **Simulator.** At 40k msg/s it uses ~0.9 core for 10k goroutines. Higher rates need either fewer, batched goroutines or several producer instances.

## Caveats

- **One machine.** Every container shares the host's CPUs, so absolute numbers would differ on separate hosts. Relative before/after comparisons on the same box are the meaningful part.
- **Redpanda dev mode.** Redpanda runs in `dev-container` mode, which relaxes fsync guarantees, so broker durability costs are not represented. Postgres uses image defaults with normal durability.
- **Point samples.** Each row is one 60 s window. `docker stats` CPU is a single sample at the end of the window, and lag is read once at the end. Two 20k samples on the post-fix code differed by ~10–25% in p50/p99.
- **Saturated latencies.** Above the ceiling, end-to-end latency grows without bound and exceeds the histogram's top buckets. Those rows report the backlog, not a stable latency.

## Reproducing

```bash
perf/loadtest.sh after-10k 10000 1000            # one load level → perf/results.jsonl
perf/profile.sh my-profile 30                    # flame graph → perf/flamegraphs/my-profile.svg
COMPOSE_PROJECT_NAME=fleetperf docker compose stop simulator ingest
docker run --rm --network fleetperf_default -v "$PWD/perf:/b" python:3.12-slim \
  sh -c "pip install -q asyncpg==0.30.0 && python /b/bench_upsert.py"
COMPOSE_PROJECT_NAME=fleetperf docker compose down -v
```
