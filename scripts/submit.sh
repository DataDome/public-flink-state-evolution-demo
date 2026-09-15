#!/usr/bin/env bash
# Builds the job if needed and submits it to the running cluster.
#
# Usage: scripts/submit.sh [savepoint-path]
#
# With a savepoint path, the job is restored from it instead of starting empty. The path is the one
# printed by scripts/savepoint.sh, and must be the in-container path (/flink-state/savepoints/...).
set -euo pipefail

cd "$(dirname "$0")/.."

JAR_NAME="flink-state-evolution-demo-1.0-SNAPSHOT.jar"
if [[ ! -f "target/$JAR_NAME" ]]; then
  echo "Job jar not found, building it..."
  mvn -q package -DskipTests
fi

RESTORE_ARGS=()
if [[ -n "${1:-}" ]]; then
  echo "Restoring from savepoint: $1"
  # --allowNonRestoredState lets the job start when the savepoint contains state for an operator
  # that no longer exists. Keep it off unless a version of the job actually removed an operator.
  RESTORE_ARGS=(--fromSavepoint "$1")
fi

docker compose -f docker/docker-compose.yml exec jobmanager \
  flink run --detached "${RESTORE_ARGS[@]}" \
  "/jars/$JAR_NAME" \
  --bootstrap-servers kafka:19092
