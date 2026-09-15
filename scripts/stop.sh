#!/usr/bin/env bash
# Stops the cluster.
#
# Usage: scripts/stop.sh [--clean]
#
# Without --clean, savepoints and checkpoints under docker/state are kept, and so are the Kafka
# topics: a savepoint taken before the restart can still be restored afterwards, which is what
# makes it possible to restart under a different state backend.
#
# With --clean, everything is discarded, including the Kafka volume.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--clean" ]]; then
  docker compose -f docker/docker-compose.yml down --volumes
  echo "Removing all checkpoints and savepoints..."
  # Flink runs as uid 9999 inside the container and creates these directories as itself, so the
  # host user cannot delete them. Removing them from inside a container sidesteps that.
  docker run --rm -v "$PWD/docker/state:/state" busybox:latest \
    sh -c 'rm -rf /state/checkpoints/* /state/savepoints/*'
  echo "Done."
else
  docker compose -f docker/docker-compose.yml down
fi
