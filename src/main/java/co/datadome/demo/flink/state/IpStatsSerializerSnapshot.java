package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.Stats;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * The name {@link StatsSerializerSnapshot} went by before {@code IpStats} was renamed to {@link Stats}. Required so
 * that Flink can reflectively instantiate the snapshot from the class-name stored in a savepoint from the previous
 * version.
 *
 * <p>It is recognized in {@link StatsSerializerSnapshot} as a compatible snapshot, as the layout is unchanged.
 */
public final class IpStatsSerializerSnapshot implements TypeSerializerSnapshot<Stats> {

    /**
     * Version of the serialized layout this snapshot describes, as written before the rename.
     */
    private int version;

    /**
     * Required on deserialization: Flink instantiates this reflectively before calling {@link #readSnapshot}.
     */
    public IpStatsSerializerSnapshot() {
        this(StatsSerializer.LATEST_VERSION);
    }

    /**
     * Used in the type-serializer to create the snapshot.
     */
    IpStatsSerializerSnapshot(int version) {
        this.version = version;
    }

    @Override
    public int getCurrentVersion() {
        return version;
    }

    @Override
    public void writeSnapshot(DataOutputView out) {
        // Only ever reached if Flink is asked to write this snapshot back, which no serializer does anymore.
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) {
        // An unknown version is accepted on purpose, so that resolveSchemaCompatibility can
        // report it rather than the restore failing here.
        version = readVersion;
    }

    /**
     * Builds a serializer under its current name.
     */
    @Override
    public TypeSerializer<Stats> restoreSerializer() {
        return new StatsSerializer(version);
    }

    /**
     * Not reached in practice: Flink calls this on the new snapshot.
     */
    @Override
    public TypeSerializerSchemaCompatibility<Stats> resolveSchemaCompatibility(TypeSerializerSnapshot<Stats> old) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "IpStatsSerializerSnapshot{version=" + version + "}";
    }
}
