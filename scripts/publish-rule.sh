#!/usr/bin/env bash
# Publishes a rule to the rules topic.
#
# Usage: scripts/publish-rule.sh <rule-id> <TOTAL_REQUESTS|ERROR_REQUESTS|DISTINCT_PATHS> <threshold> [enabled]
#
# Examples:
#   scripts/publish-rule.sh too-many-errors ERROR_REQUESTS 20
#   scripts/publish-rule.sh too-many-errors ERROR_REQUESTS 20 false   # removes it again
set -euo pipefail

if [[ $# -lt 3 ]]; then
  sed -n '2,10p' "$0"
  exit 1
fi

RULE_ID="$1"
METRIC="$2"
THRESHOLD="$3"
ENABLED="${4:-true}"

case "$METRIC" in
  TOTAL_REQUESTS|ERROR_REQUESTS|DISTINCT_PATHS) ;;
  *)
    echo "Unknown metric: '$METRIC'" >&2
    exit 1
    ;;
esac

cd "$(dirname "$0")/.."

RULE="{\"ruleId\":\"$RULE_ID\",\"metric\":\"$METRIC\",\"threshold\":$THRESHOLD,\"isEnabled\":$ENABLED}"
echo "Publishing: $RULE"

echo "$RULE" | docker compose -f docker/docker-compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 --topic rules
