#!/usr/bin/env bash
# Starts Kafka and the Flink session cluster, with the chosen state backend.
#
# Usage: scripts/start.sh [hashmap|rocksdb]
set -euo pipefail

BACKEND="${1:-hashmap}"
case "$BACKEND" in
  hashmap|rocksdb) ;;
  *)
    echo "Unknown state backend: '$BACKEND' (expected 'hashmap' or 'rocksdb')" >&2
    exit 1
    ;;
esac

cd "$(dirname "$0")/.."

# The Flink containers run as uid 9999, while this bind-mounted directory belongs to the host
# user, so Flink could not create its checkpoint and savepoint directories inside it otherwise.
mkdir -p docker/state/checkpoints docker/state/savepoints
# Only the three directories created just above: anything below them was created by the container
# and is owned by uid 9999, which the host user is not allowed to chmod.
chmod 777 docker/state docker/state/checkpoints docker/state/savepoints 2>/dev/null || true

echo "Starting the cluster with the '$BACKEND' state backend..."
STATE_BACKEND="$BACKEND" docker compose -f docker/docker-compose.yml up -d

echo
echo "Flink dashboard: http://localhost:8081"
echo "Kafka broker:    localhost:9092"
