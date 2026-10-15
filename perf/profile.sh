#!/bin/sh
# Flame graph of the running ingest process without modifying its image:
# py-spy runs in a sidecar that joins the ingest container's PID namespace.
#
#   perf/profile.sh OUTPUT_NAME [SECONDS]
set -e
cd "$(dirname "$0")"
docker build -q -t fleet-pyspy -f pyspy.Dockerfile . >/dev/null
docker run --rm --pid=container:fleetperf-ingest-1 --cap-add SYS_PTRACE -v "$PWD/flamegraphs:/out" \
  fleet-pyspy py-spy record -p 1 -d "${2:-30}" -r 200 --nonblocking -o "/out/$1.svg"
