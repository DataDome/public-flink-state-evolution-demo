# State evolution notes

Scaffold for the talk. The list of changes to demonstrate is deliberately left open — fill in the
ones you actually want to show.

## What this job does so that it can be restored at all

These are the decisions in the code that exist purely to make restoring a savepoint possible. They
are easy to miss, and each one fails in its own way when it is missing.

| Decision | Where | What breaks without it |
|---|---|---|
| Explicit `uid()` on every operator | `BehavioralAnalysisJob` | Flink generates operator ids from the job graph. Any change to the graph changes the ids, and the savepoint no longer matches any operator. |
| `pipeline.auto-generate-uids: false` | `BehavioralAnalysisJob.config()` | Nothing stops you forgetting a `uid()`. The job runs happily, and the problem only shows up at the next restore. |
| `pipeline.generic-types: false` | `BehavioralAnalysisJob.config()` | A type Flink cannot treat as a POJO silently falls back to Kryo, which cannot evolve. The job runs, the savepoint is written, and the restore fails later. |
| Strict POJOs for all state | `model/` | Same as above: the failure is silent until it is not. |
| `StateSerializationTest` | `src/test/java/.../model` | Nothing catches a field that quietly turns a POJO into a generic type. |
| Canonical savepoint format | `scripts/savepoint.sh` | Native savepoints are tied to the state backend that wrote them, so hashmap → rocksdb would not restore. |

## The state that actually exists

`IpStatsFunction`, keyed by IP address:

- `ValueState<IpStats> ipStats` — the record in `model/IpStats.java`. Note that it stores the
  request and error *counts*, never the error ratio: a ratio cannot be accumulated, so it is
  derived on read.
- `MapState<String, Boolean> seenPaths` — one entry per distinct path seen.

`RuleEvaluationFunction`, keyed by IP address, plus broadcast state:

- `MapState<String, Rule> rules` (broadcast) — the current rule set.
- `MapState<String, Boolean> firedRuleIds` — rules that already fired for the current session.
- `ValueState<Long> sessionStart`, `ValueState<Long> lastSeen`.

## Candidate changes to demonstrate

To fill in. Rough ordering from "just works" to "does not work":

- **Add a field** to `IpStats` (for example a count of distinct user agents). Supported by
  `PojoSerializer`; the new field comes back as its default value for existing keys.
- **Remove a field** from `IpStats`. Also supported; the old value is dropped on restore.
- **Add a constant** to the `Metric` enum. Supported by `EnumSerializer`, and worth showing because
  `Metric` lives inside the broadcast state rather than in the keyed state. *Renaming* a constant
  is not supported, which is worth contrasting: the metric names encode their comparison direction
  (`..._AT_LEAST`, `..._AT_MOST`), so renaming one is a tempting change that would break restore.
- **Widen a field**, `int distinctPathCount` to `long`. Not a supported POJO evolution: Flink
  matches fields by name *and* type, so this reads as "drop one field, add another".
- **Rename a field**. Indistinguishable from removing one field and adding another, so the
  accumulated value is silently lost — arguably the most dangerous case, because nothing fails.
- **Replace `MapState<String, Boolean> seenPaths` with a collection field inside `IpStats`.** This
  is the one that cannot work: a `Set<String>` field makes `IpStats` a generic type, which means
  Kryo, which means no evolution at all. With `pipeline.generic-types: false` the job refuses to
  start, which is the point.

## Two things worth mentioning on stage

**Why not `StateTtlConfig`.** Expiring sessions with TTL instead of timers would be three lines
instead of the `onTimer` logic in both operators. It is avoided on purpose: TTL wraps the state
serializer in a TTL-aware decorator, which changes the format on disk and adds a second variable to
every compatibility question. The explicit timers keep the serialized schema exactly the POJO on
the slide.

**hashmap versus rocksdb.** Use `scripts/start.sh rocksdb`. The same canonical savepoint restores
under both, but they do not fail the same way when the schema no longer matches: the hashmap
backend deserializes all state eagerly at restore, so an incompatible change fails immediately and
loudly; RocksDB deserializes lazily, per key, on access — so the restore succeeds and the failure
surfaces later, on the first request that touches a broken key.
