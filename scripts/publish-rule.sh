#!/usr/bin/env bash
# Publishes a rule to the rules topic.
#
# Usage: scripts/publish-rule.sh <rule-id> <metric> <threshold> <min-total-requests> [enabled]
#
# Metrics, each of which states the direction of its own comparison:
#   TOTAL_REQUESTS_AT_LEAST   fires when the session's request count is >= threshold
#   ERROR_RATIO_AT_LEAST      fires when the share of failed requests (0 to 1) is >= threshold
#   DISTINCT_PATHS_AT_MOST    fires when the number of distinct paths is <= threshold
#
# A rule is only evaluated once the session has at least <min-total-requests> requests: a single
# failed request is a 100% error ratio, and one request means one distinct path.
#
# Examples:
#   scripts/publish-rule.sh failing-a-lot ERROR_RATIO_AT_LEAST 0.5 50
#   scripts/publish-rule.sh one-path-only DISTINCT_PATHS_AT_MOST 2 50
#   scripts/publish-rule.sh failing-a-lot ERROR_RATIO_AT_LEAST 0.5 50 false   # removes it again
set -euo pipefail

usage() {
  # Prints the comment block at the top of this file, whatever length it happens to be.
  awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"
}

if [[ $# -lt 4 ]]; then
  usage
  exit 1
fi

RULE_ID="$1"
METRIC="$2"
THRESHOLD="$3"
MIN_TOTAL_REQUESTS="$4"
ENABLED="${5:-true}"

case "$METRIC" in
  TOTAL_REQUESTS_AT_LEAST|ERROR_RATIO_AT_LEAST|DISTINCT_PATHS_AT_MOST) ;;
  *)
    echo "Unknown metric: '$METRIC'" >&2
    echo "Expected TOTAL_REQUESTS_AT_LEAST, ERROR_RATIO_AT_LEAST or DISTINCT_PATHS_AT_MOST." >&2
    exit 1
    ;;
esac

if ! [[ "$THRESHOLD" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
  echo "Threshold must be a number: '$THRESHOLD'" >&2
  exit 1
fi

if ! [[ "$MIN_TOTAL_REQUESTS" =~ ^[0-9]+$ ]]; then
  echo "Minimum total requests must be a whole number: '$MIN_TOTAL_REQUESTS'" >&2
  exit 1
fi

cd "$(dirname "$0")/.."

RULE="{\"ruleId\":\"$RULE_ID\",\"metric\":\"$METRIC\",\"threshold\":$THRESHOLD,\"minTotalRequests\":$MIN_TOTAL_REQUESTS,\"enabled\":$ENABLED}"
echo "Publishing: $RULE"

echo "$RULE" | docker compose -f docker/docker-compose.yml exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 --topic rules
