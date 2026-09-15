#!/usr/bin/env bash
# Runs the demo traffic generator against the local Kafka.
#
# Traffic is mostly ordinary, with one IP address (10.0.0.66) behaving like a credential stuffing
# bot so that the demo rules have something to fire on. Runs until interrupted.
set -euo pipefail

cd "$(dirname "$0")/.."

JAR="target/flink-state-evolution-demo-1.0-SNAPSHOT.jar"
if [[ ! -f "$JAR" ]]; then
  echo "Job jar not found, building it..."
  mvn -q package -DskipTests
fi

# The shaded jar already bundles kafka-clients and Jackson, so no other classpath is needed.
exec java -cp "$JAR" co.datadome.demo.flink.tools.DemoDataGenerator \
  --bootstrap-servers localhost:9092 --topic http-requests
