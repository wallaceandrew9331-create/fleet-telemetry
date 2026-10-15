#!/bin/sh
# One load level: fresh stack and database, settle, then a windowed snapshot.
#
#   perf/loadtest.sh LABEL ASSETS INTERVAL_MS [SETTLE_S] [WINDOW_S]
#
# Uses its own compose project so the default stack's volume is untouched.
# Tear down afterwards with: COMPOSE_PROJECT_NAME=fleetperf docker compose down -v
set -e
cd "$(dirname "$0")/.."
export COMPOSE_PROJECT_NAME=fleetperf ASSET_COUNT="$2" INTERVAL_MS="$3"
docker compose down -v >/dev/null 2>&1
docker compose up --build -d >/dev/null 2>&1
python -c "import time; time.sleep(${4:-120})"
python perf/measure.py "$1" "${5:-60}" perf/results.jsonl
