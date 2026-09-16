package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.Stats;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * The name {@link StatsSerializerSnapshot} went by before {@code IpStats} was renamed to
 * {@link Stats}.
 *
 * <p>Flink writes the snapshot's class name into the savepoint and instantiates it reflectively on
 * restore, so a savepoint taken before the rename asks for this exact name. Deleting the class
 * fails the restore in the class loader, before any compatibility check runs at all.
 *
 * <p>Keeping it is only half of what the rename costs: {@link StatsSerializerSnapshot} also has to
 * recognize it, or the restore fails one step later with an incompatibility instead.
 *
 * <p>Note what did <em>not</em> need this treatment. The serializer class was renamed freely,
 * because the savepoint never records its name — it is reached through {@link #restoreSerializer()}.
 * Neither does the record class, but only because this state is written by hand: had it been left
 * to {@code PojoSerializer}, which does record the class name, renaming {@code IpStats} would have
 * been the breaking change instead.
 *
 * <p>The layout is unchanged, so this describes exactly what {@link StatsSerializer} reads today.
 */
public final class IpStatsSerializerSnapshot implements TypeSerializerSnapshot<Stats> {

    /** Layout version of the state being restored, as written before the rename. */
    private int version;

    /** Required: Flink instantiates this reflectively before calling {@link #readSnapshot}. */
    public IpStatsSerializerSnapshot() {
        this(StatsSerializer.LATEST_VERSION);
    }

    IpStatsSerializerSnapshot(int version) {
        this.version = version;
    }

    @Override
    public int getCurrentVersion() {
        return version;
    }

    @Override
    public void writeSnapshot(DataOutputView out) {
        // Deliberately empty, as in StatsSerializerSnapshot. Only ever reached if Flink is asked to
        // write this snapshot back, which no serializer does anymore: state restored through this
        // class is written out again under the current name.
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) {
        version = readVersion;
    }

    /** Builds a serializer for the layout this snapshot describes, under its current name. */
    @Override
    public TypeSerializer<Stats> restoreSerializer() {
        return new StatsSerializer(version);
    }

    /** Not reached in practice: Flink calls this on the new snapshot. */
    @Override
    public TypeSerializerSchemaCompatibility<Stats> resolveSchemaCompatibility(
            TypeSerializerSnapshot<Stats> oldSerializerSnapshot) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }

    @Override
    public String toString() {
        return "IpStatsSerializerSnapshot{version=" + version + "}";
    }
}
