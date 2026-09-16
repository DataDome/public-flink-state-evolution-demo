package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.IpStats;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Serialized state of the type-serializer itself.
 */
public final class IpStatsSerializerSnapshot implements TypeSerializerSnapshot<IpStats> {

    /**
     * Version of the serialized layout this snapshot describes, passed by the {@link IpStatsSerializer}, or read from
     * the savepoint when restoring the state.
     */
    private int version;

    /**
     * Required on deserialization: Flink instantiates this reflectively before calling {@link #readSnapshot}.
     */
    public IpStatsSerializerSnapshot() {
        this(IpStatsSerializer.LATEST_VERSION);
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
        // Nothing to do: IpStatsSerializer doesn't have any state apart from the version, directly handled by Flink.
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) {
        // An unknown version is accepted on purpose, so that resolveSchemaCompatibility can
        // report it rather than the restore failing here.
        version = readVersion;
    }

    @Override
    public TypeSerializer<IpStats> restoreSerializer() {
        return new IpStatsSerializer(version);
    }

    /**
     * Decides whether state described by that snapshot can be read by the serializer this snapshot
     * belongs to.
     *
     * <p>Note the direction: this is the snapshot of the serializer the job wants to use now, and
     * that argument is the one restored from the savepoint.
     */
    @Override
    public TypeSerializerSchemaCompatibility<IpStats> resolveSchemaCompatibility(TypeSerializerSnapshot<IpStats> old) {

        if (old instanceof IpStatsSerializerSnapshot that) {
            if (that.version == version) {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            } else if (that.version == IpStatsSerializer.PREVIOUS_VERSION && version == IpStatsSerializer.LATEST_VERSION) {
                // This is the snapshot for the latest version, and restoring from the savepoint gave us a snapshot for the previous version.
                // The layout has changed, and the serializer can handle it.
                return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
            } else {
                // Other versions cannot be handled => incompatible
                return TypeSerializerSchemaCompatibility.incompatible();
            }
        }

        // The state was written by a serializer we don't recognize, and can't read.
        return TypeSerializerSchemaCompatibility.incompatible();
    }

    @Override
    public String toString() {
        return "IpStatsSerializerSnapshot{version=" + version + "}";
    }
}
