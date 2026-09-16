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

- `ValueState<Stats> ipStats` — the record in `model/Stats.java`, written by the custom
  `state/StatsSerializer` rather than by `PojoSerializer`. Note that it stores the request and
  error *counts*, never the error ratio: a ratio cannot be accumulated, so it is derived on read.
- `MapState<String, Boolean> seenPaths` — one entry per distinct path seen.

`RuleEvaluationFunction`, keyed by IP address, plus broadcast state:

- `MapState<String, Rule> rules` (broadcast) — the current rule set.
- `MapState<String, Boolean> firedRuleIds` — rules that already fired for the current session.
- `ValueState<Long> sessionStart`, `ValueState<Long> lastSeen`.

## The custom serializer

`state/StatsSerializer` is not necessary — Flink serializes `Stats` on its own. It is there to
show what has to be written by hand once you take that job over, and it is registered explicitly on
the state descriptor in `IpStatsFunction`.

It only governs **state**. Records travelling between the two operators are serialized from the
type information, so those still go through `PojoSerializer`. That is why `StateSerializationTest`
still matters: it covers the wire format, not the state.

### One version number, carried by the snapshot

The record layout is the serializer's entire configuration, so the layout version *is* the
snapshot version. `StatsSerializerSnapshot.getCurrentVersion()` returns it and `writeSnapshot`
writes nothing, because Flink already persists that number itself:

```java
static void writeVersionedSnapshot(DataOutputView out, TypeSerializerSnapshot<?> snapshot) {
    out.writeUTF(snapshot.getClass().getName());
    out.writeInt(snapshot.getCurrentVersion());   // <- the layout version
    snapshot.writeSnapshot(out);                  // <- nothing left to write
}
```

On restore Flink reads that int back and hands it to `readSnapshot` as `readVersion`. Nothing is
written per record, so the version costs no bytes in state.

`writeSnapshot` only earns its keep once a serializer has configuration beyond the layout — nested
serializer snapshots, registered classes, and so on, which is what `PojoSerializerSnapshot` and
`KryoSerializerSnapshot` put there.

An unknown version is deliberately *not* rejected in `readSnapshot`: throwing there fails the
restore with an IO error, whereas accepting it lets `resolveSchemaCompatibility` report a proper
incompatibility.

### What it answers on restore

`resolveSchemaCompatibility` is called on the snapshot of the serializer the job wants to use
*now*, with the snapshot from the savepoint as the argument. Note the direction — it was reversed
in Flink 1.19.

| State was written with | Answer |
|---|---|
| the same layout | `compatibleAsIs()` |
| the same layout, under the snapshot class's previous name | `compatibleAsIs()` |
| any other layout version | `incompatible()` — only one layout exists so far |
| a different serializer, e.g. `PojoSerializer` | `incompatible()` |

That last row is worth showing: **introducing this serializer is itself a breaking change.** A
savepoint taken before it existed cannot be restored, because the bytes were laid out by
`PojoSerializer` and nothing here can read them.

### What a rename costs

`IpStats` was renamed to `Stats`, and its serializer and snapshot with it. Three class names were
involved and only one of them is written into the savepoint:

| Renamed | In the savepoint | Consequence |
|---|---|---|
| the record, `IpStats` → `Stats` | no — the custom serializer writes fields, not a class name | free here. Under `PojoSerializer` it would *not* be: that one records the class name, and this is the rename that would break |
| the serializer, `IpStatsSerializer` → `StatsSerializer` | no — it is reached through `restoreSerializer()` | free |
| the snapshot, `IpStatsSerializerSnapshot` → `StatsSerializerSnapshot` | **yes** — `writeVersionedSnapshot` writes `snapshot.getClass().getName()` | breaking |

So `IpStatsSerializerSnapshot` stays on the classpath, reduced to a shim that hands back a
`StatsSerializer`. Without it the restore fails in the class loader, before any compatibility check
runs.

Keeping the class is only half of it, and this is the part that is easy to get wrong: Flink then
hands that old snapshot to `StatsSerializerSnapshot.resolveSchemaCompatibility`, whose `instanceof`
check no longer matches its own name. The restore fails one step later, with an incompatibility
instead of a `ClassNotFoundException`. The new snapshot has to answer to **both** names.

The state descriptor name (`"ipStats"`) is a fourth name, and it identifies the state itself rather
than how it is written. It is left alone: renaming it would leave the accumulated state behind
under the old name, silently.

### Adding a field to a custom serializer

Only one layout exists today, version `StatsSerializer.LATEST_VERSION`. The version is persisted
anyway, and an instance already carries the version it was built for — `LATEST_VERSION`
normally, or
whatever came out of the savepoint when `restoreSerializer()` built it. That is the groundwork; a
second layout then needs:

1. The new field on `Stats`.
2. `LATEST_VERSION` bumped to 2, with the previous value kept as a named constant.
3. A branch on `version` in `serialize` and `deserialize`: write the new field only for version 2,
   and read it with an explicit default when the instance is reading version 1.
4. `compatibleAfterMigration()` from `resolveSchemaCompatibility` when the savepoint's version is
   the older one. Flink then reads with `restoreSerializer()` and writes back with the current
   serializer.

`StatsSerializerTest` covers each answer in the table above, including that an unknown layout
version is reported rather than throwing while the snapshot is read.

## Candidate changes to demonstrate

To fill in. Rough ordering from "just works" to "does not work":

- **Add a field** to `Stats` (for example a count of distinct user agents). Supported by
  `PojoSerializer`; the new field comes back as its default value for existing keys.
- **Remove a field** from `Stats`. Also supported; the old value is dropped on restore.
- **Add a constant** to the `Metric` enum. Supported by `EnumSerializer`, and worth showing because
  `Metric` lives inside the broadcast state rather than in the keyed state. *Renaming* a constant
  is not supported, which is worth contrasting: the metric names encode their comparison direction
  (`..._AT_LEAST`, `..._AT_MOST`), so renaming one is a tempting change that would break restore.
- **Widen a field**, `int distinctPathCount` to `long`. Not a supported POJO evolution: Flink
  matches fields by name *and* type, so this reads as "drop one field, add another".
- **Rename a field**. Indistinguishable from removing one field and adding another, so the
  accumulated value is silently lost — arguably the most dangerous case, because nothing fails.
- **Rename the record class, its serializer and its snapshot.** Only the snapshot's name is
  recorded in the savepoint — see above. Worth showing because the one class that has to survive
  the rename is not the one people expect, and because keeping it is not enough on its own.
- **Add a field without bumping the layout version.** The serializer silently writes the new field
  and reads state that does not contain it, so deserialization runs off the end of the record or
  reads the following field's bytes. There is no error and the values are simply wrong, which makes
  it the best argument for why the version constant exists.
- **Replace `MapState<String, Boolean> seenPaths` with a collection field inside `Stats`.** This
  is the one that cannot work: a `Set<String>` field makes `Stats` a generic type, which means
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
