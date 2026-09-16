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

- `ValueState<IpStats> ipStats` — the record in `model/IpStats.java`, written by the custom
  `state/IpStatsSerializer` rather than by `PojoSerializer`. Note that it stores the request and
  error *counts*, never the error ratio: a ratio cannot be accumulated, so it is derived on read.
- `MapState<String, Boolean> seenPaths` — one entry per distinct path seen.

`RuleEvaluationFunction`, keyed by IP address, plus broadcast state:

- `MapState<String, Rule> rules` (broadcast) — the current rule set.
- `MapState<String, Boolean> firedRuleIds` — rules that already fired for the current session.
- `ValueState<Long> sessionStart`, `ValueState<Long> lastSeen`.

## The custom serializer

`state/IpStatsSerializer` is not necessary — Flink serializes `IpStats` on its own. It is there to
show what has to be written by hand once you take that job over, and it is registered explicitly on
the state descriptor in `IpStatsFunction`.

It only governs **state**. Records travelling between the two operators are serialized from the
type information, so those still go through `PojoSerializer`. That is why `StateSerializationTest`
still matters: it covers the wire format, not the state.

What it costs is the evolution that came for free. `PojoSerializer` carries per-field metadata, and
that metadata is exactly what lets it match the fields in a savepoint against the fields in the
class. Writing the fields as primitives in a fixed order leaves nothing to match against: adding,
removing or reordering one would keep reading old bytes with the new layout, and the fields would
simply shift. What makes that detectable again is a version number.

### Two versions, kept apart

| | What it versions | Bumped when |
|---|---|---|
| `getCurrentVersion()`, i.e. `SNAPSHOT_VERSION` | the snapshot's own format, i.e. the bytes `writeSnapshot` writes | the snapshot itself starts writing something different |
| the `int` inside `writeSnapshot` | the layout of `IpStats` on disk | a field is added, removed or reordered |

Flink persists the first one itself, and allow us to write our payload:

```java
static void writeVersionedSnapshot(DataOutputView out, TypeSerializerSnapshot<?> snapshot) {
    out.writeUTF(snapshot.getClass().getName());
    out.writeInt(snapshot.getCurrentVersion());   // <- SNAPSHOT_VERSION
    snapshot.writeSnapshot(out);                  // <- the layout version, as a payload of its own
}
```

On restore, Flink reads that int back and hands it to `readSnapshot` as `readVersion`, and the layout
version comes out of the payload. Nothing is written per record, so both cost 8 bytes in state.

The two versions are also rejected differently, which is the point of separating them. An unknown
`readVersion` means the savepoint was written by a newer version of `IpStatsSerializerSnapshot`
than the one running, and there is no way to parse what follows: `readSnapshot` throws. An unknown
*layout* version parses fine, so it is deliberately accepted and left to
`resolveSchemaCompatibility`, which reports a proper incompatibility instead of an IO error.

### What it answers on restore

`resolveSchemaCompatibility` is called on the snapshot of the serializer the job wants to use
*now*, with the snapshot from the savepoint as the argument. Note the direction — it was reversed
in Flink 1.19 by FLIP-263.

| State was written with | Answer |
|---|---|
| the same layout version | `compatibleAsIs()` |
| any other layout version | `incompatible()` — only one layout exists so far |
| a different serializer, e.g. `PojoSerializer` | `incompatible()` |

That last row is worth showing: **introducing this serializer is itself a breaking change.** A
savepoint taken before it existed cannot be restored, because the bytes were laid out by
`PojoSerializer` and nothing here can read them.

### Adding a field to a custom serializer

Only one layout exists today, version `IpStatsSerializer.LATEST_VERSION`. The version is persisted
anyway, and an instance already carries the version it was built for — `LATEST_VERSION` normally,
or whatever came out of the savepoint when `restoreSerializer()` built it. That is the groundwork;
a second layout then needs:

1. The new field on `IpStats`.
2. `LATEST_VERSION` bumped to 2, with the previous value kept as a named constant.
3. A branch on `version` in `serialize` and `deserialize`: write the new field only for version 2,
   and read it with an explicit default when the instance is reading version 1.
4. `compatibleAfterMigration()` from `resolveSchemaCompatibility` when the savepoint's version is
   the older one. Flink then reads with `restoreSerializer()` and writes back with the current
   serializer.

Forget step 4 and the restore fails loudly, which is what writing the version down buys. Forget
step 3, and it does not: the version says the bytes are readable, and the code reads them wrong.

`IpStatsSerializerTest` covers each answer in the table above, including that an unknown layout
version is reported rather than throwing while the snapshot is read.

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
- **Add a field without bumping the layout version.** The serializer silently writes the new field
  and reads state that does not contain it, so deserialization runs off the end of the record or
  reads the following field's bytes. There is no error and the values are simply wrong, which makes
  it the best argument for why the version constant exists.
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
