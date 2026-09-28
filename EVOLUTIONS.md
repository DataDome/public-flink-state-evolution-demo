# Managing state evolutions

## Decisions

These are some important decisions made to make this project a good example of managing state evolutions:

### Explicit `uid()` on every operator
In: `BehavioralAnalysisJob`.

Flink generates operator ids from the job graph. Any change to the graph changes the ids, and the savepoint no longer matches any operator.

To enforce this, we set `pipeline.auto-generate-uids: false` in `BehavioralAnalysisJob.config()`.

### No Kryo fallback
In: `BehavioralAnalysisJob.config()`

We want to either use the baked-in Flink serialization, or custom serializers.
Kryo doesn't allow for structural evolutions.

To enforce this, we set `pipeline.generic-types: false` in `BehavioralAnalysisJob.config()`.

### Canonical savepoint format
In: `scripts/savepoint.sh`

Having canonical savepoints lets you change the backend and restart from a savepoint.


## Existing state

### In `IpStatsFunction`
All states keyed by IP address.

- `ValueState<IpStats> ipStats`
  - Written by the custom `IpStatsSerializer` rather than by `PojoSerializer`
- `MapState<String, Boolean> seenPaths`
  - One entry per distinct path seen

### In `RuleEvaluationFunction`
Operator states keyed by IP address, plus a broadcast state.

- `MapState<String, Rule> rules` _(broadcast)_: the current rule set
- `MapState<String, Boolean> firedRuleIds`: rules that already fired for the current session
- `ValueState<Long> sessionStart`, `ValueState<Long> lastSeen`: timestamps to handle session expiring and coming back

### Custom serializer `IpStatsSerializer`

It is not strictly necessary, as `IpStats` is a POJO. It is here to demonstrate usage of a custom serializer. It is registered explicitly on the state descriptor for `ipStats` in `IpStatsFunction`.

It only governs **state**. Records travelling between the two operators are serialized from the type information, so
those still go through `PojoSerializer`.

In the `main` branch, it contains a single version (`IpStatsSerializer.LATEST_VERSION`), which it never needs to check
(since there's no other version for which we need to deserialize differently). Different versions appear on the various
change branches.

## Demonstrated evolutions

### Change a field in a class
**On branch: `use-case-change-field-type`**

This branch changes `errorCount` from a `long` to an `int`.

The type-serializer therefore has to handle two different versions: the old one (with a `long`) and the current one
(with an `int`). In `StatsSerializerSnapshot.resolveSchemaCompatibility`, the old version appears as requiring a
migration.

Adding, removing, renaming or moving fields around follow a very similar pattern. Almost all changes inside the
structure of the class can be handled that way.


### Rename class
**On branch: `use-case-rename-a-class`**

This branch renames the `IpStats` class to `Stats`, and its serializer and snapshot with it.

Note that the old `IpStatsSerializerSnapshot` must be kept, as the TS-Snapshot is restored by class-name from the
savepoint. So, it needs to stay on the classpath, and hands back a `StatsSerializer` (so we don't have to keep
`IpStatsSerializer`). In `StatsSerializerSnapshot.resolveSchemaCompatibility`, we handle both snapshot classes as
compatible.

Note that we don't change the state descriptor name (`"ipStats"`): renaming it would drop the old state.

### Others
Those don't exist yet, but should be added progressively.

- Move from PojoSerializer to a custom serializer.

