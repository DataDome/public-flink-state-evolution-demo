# public-flink-state-evolution-demo

Code for the Flink meetup: Handling state evolution.

A deliberately simplified behavioral analysis pipeline, built with **Apache Flink 2.3.0** and
**Java 21**, whose real purpose is to have some long-lived keyed state worth evolving between two
versions of a job.

```
  http-requests ──▶ [ IP statistics ] ──▶ stats ──▶ [ Rule evaluation ] ──▶ rule-matches
  rules ─────────────────────────── broadcast ────────▶      ▲
```

- **IP statistics** (`IpStatsFunction`) accumulates per-IP counters over a session, and emits the
  updated statistics on every request. There is no window: statistics accumulate for as long as
  requests keep arriving for that IP, and are discarded after 24 hours of inactivity.
- **Rule evaluation** (`RuleEvaluationFunction`) receives those statistics on its keyed input and
  the rules on a broadcast input, and emits a match when a rule's threshold is reached. A rule
  fires at most once per IP per session.

## Data on the topics

All three topics carry JSON.

`http-requests`:

```json
{"timestampMs":1789404388370,"ip":"10.0.0.11","path":"/cart","userAgent":"curl/8.5.0","statusCode":500}
```

`rules` — a changelog: publishing the same `ruleId` again replaces it, publishing it with
`"isEnabled":false` removes it.

```json
{"ruleId":"failing-a-lot","metric":"ERROR_RATIO_AT_LEAST","threshold":0.5,"minTotalRequests":50,"isEnabled":true}
```

A rule selects one of the statistics the pipeline already computes; it cannot filter individual
requests, because the statistics are aggregated before any rule is known. Each metric states the
direction of its own comparison, so that no rule depends on an unwritten convention:

| `metric` | Fires when | Threshold |
|---|---|---|
| `TOTAL_REQUESTS_AT_LEAST` | request count `>=` threshold | a count |
| `ERROR_RATIO_AT_LEAST` | share of failed requests `>=` threshold | between 0 and 1 |
| `DISTINCT_PATHS_AT_MOST` | distinct path count `<=` threshold | a count |

Both directions are inclusive. `minTotalRequests` is the number of requests a session must have
before the rule is evaluated at all: a single failed request is a 100% error ratio and means
nothing, and without the guard `DISTINCT_PATHS_AT_MOST` would match every session on its first
request.

`rule-matches`:

```json
{"ruleId":"failing-a-lot","ip":"10.0.0.66","metric":"ERROR_RATIO_AT_LEAST","observedValue":0.79,"threshold":0.5,"detectedAtMs":1789404498084}
```

## Requirements

Java 21, Maven, Docker with Compose.

## Running it

```bash
mvn package                     # build and run the tests
./scripts/start.sh              # Kafka + a Flink session cluster (hashmap backend)
./scripts/submit.sh             # build if needed, then submit the job
./scripts/generate-traffic.sh   # fake traffic, in its own terminal
./scripts/watch-matches.sh      # tail the matches, in another terminal

# fires on the bot: it fails about 80% of its requests, everyone else about 5%
./scripts/publish-rule.sh failing-a-lot ERROR_RATIO_AT_LEAST 0.5 50

# fires on the bot too, for a different reason: it only ever hits /login
./scripts/publish-rule.sh one-path-only DISTINCT_PATHS_AT_MOST 2 50
```

Within a few seconds, `10.0.0.66` — which behaves like a credential stuffing bot — crosses the
threshold and a match appears. The Flink dashboard is on <http://localhost:8081>.

To remove a rule again:

```bash
./scripts/publish-rule.sh failing-a-lot ERROR_RATIO_AT_LEAST 0.5 50 false
```

## Savepoint and restore

```bash
./scripts/savepoint.sh          # savepoint the running job, keep it running
./scripts/savepoint.sh --stop   # savepoint and stop, which is what you want before deploying v2
./scripts/submit.sh /flink-state/savepoints/savepoint-xxxx-yyyy
```

Savepoints are written in **canonical** format on purpose, so that the same savepoint can be
restored under either state backend. They land in `docker/state/savepoints/`, which is bind-mounted
and survives `scripts/stop.sh`.

## Switching state backend

```bash
./scripts/savepoint.sh --stop
./scripts/stop.sh                # keeps the Kafka topics and the savepoints
./scripts/start.sh rocksdb
./scripts/submit.sh /flink-state/savepoints/savepoint-xxxx-yyyy
```

`hashmap` and `rocksdb` do not fail the same way when the state schema no longer matches — see
[docs/state-evolution.md](docs/state-evolution.md).

## Tearing down

```bash
./scripts/stop.sh            # stop, keep Kafka data and savepoints
./scripts/stop.sh --clean    # stop and discard everything
```

## Notes

- **Counts grow, ratios do not.** A session has no window, so `TOTAL_REQUESTS_AT_LEAST` increases
  monotonically and is eventually crossed by every IP address, given enough traffic. That is why
  the interesting rules are the other two: an error ratio is scale-free and stays put, and a
  distinct path count separates the bot structurally. `TOTAL_REQUESTS_AT_LEAST` is still useful,
  but treat it as "this session is big", not as a detection on its own.
- The Kafka connector is `5.0.0-2.2`: no build against Flink 2.3 has been released yet. It only
  uses `@Public` API and declares its Flink dependencies as `provided`, so it introduces no
  conflicting jars.
- The job jar bundles `slf4j`, which a Flink job jar normally should not. It is there so that
  `DemoDataGenerator` can run as a plain `java` process; Flink loads `org.slf4j` parent-first, so
  the bundled copies are ignored on the cluster.
