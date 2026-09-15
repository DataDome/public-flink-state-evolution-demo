#!/usr/bin/env bash
# Tails the rule matches produced by the job.
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose -f docker/docker-compose.yml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 --topic rule-matches --from-beginning
