#!/usr/bin/env bash
# Takes a savepoint of the running job, and prints its path.
#
# Usage: scripts/savepoint.sh [--stop]
#
# The savepoint is taken in CANONICAL format on purpose. Canonical savepoints are independent of
# the state backend that wrote them, which is what makes it possible to take a savepoint under
# hashmap and restore it under rocksdb. The faster '--type native' format is not portable that way.
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f docker/docker-compose.yml)

JOB_ID="$("${COMPOSE[@]}" exec -T jobmanager flink list --running 2>/dev/null \
  | grep -oE '[0-9a-f]{32}' | head -1)"

if [[ -z "$JOB_ID" ]]; then
  echo "No running job found." >&2
  exit 1
fi

if [[ "${1:-}" == "--stop" ]]; then
  echo "Stopping job $JOB_ID with a savepoint..."
  "${COMPOSE[@]}" exec -T jobmanager \
    flink stop --type canonical --savepointPath /flink-state/savepoints "$JOB_ID"
else
  echo "Taking a savepoint of job $JOB_ID..."
  "${COMPOSE[@]}" exec -T jobmanager \
    flink savepoint --type canonical "$JOB_ID" /flink-state/savepoints
fi

echo
echo "Savepoints now available:"
ls -1dt docker/state/savepoints/*/ 2>/dev/null | head -5
