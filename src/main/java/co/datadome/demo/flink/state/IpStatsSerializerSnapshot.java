package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.IpStats;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * What Flink stores alongside the state so that it can work out, on restore, whether the state can
 * still be read.
 */
public final class IpStatsSerializerSnapshot implements TypeSerializerSnapshot<IpStats> {

    /**
     * Version of the record layout this snapshot describes. Exposed through
     * {@link #getCurrentVersion()}, which is what gets persisted and read back.
     */
    private int version;

    /** Required: Flink instantiates this reflectively before calling {@link #readSnapshot}. */
    public IpStatsSerializerSnapshot() {
        this(IpStatsSerializer.LATEST_VERSION);
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
        // Deliberately empty: the version is reported through getCurrentVersion() instead.
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        // An unknown version is accepted on purpose, so that resolveSchemaCompatibility can
        // report it rather than the restore failing here.
        version = readVersion;
    }

    /** Builds a serializer that handles the layout this snapshot describes. */
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
    public TypeSerializerSchemaCompatibility<IpStats> resolveSchemaCompatibility(
            TypeSerializerSnapshot<IpStats> oldSerializerSnapshot) {

        if (!(oldSerializerSnapshot instanceof IpStatsSerializerSnapshot that)) {
            // The state was written by a different serializer altogether, PojoSerializer for
            // instance. Nothing here knows how to read it.
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        if (that.version == version) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }

        if (version == IpStatsSerializer.LATEST_VERSION
                && that.version == IpStatsSerializer.PREVIOUS_VERSION) {
            // Flink reads the old records with restoreSerializer() and writes them back with the
            // current one, which is what turns the long error counts into ints.
            return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
        }

        return TypeSerializerSchemaCompatibility.incompatible();
    }

    @Override
    public String toString() {
        return "IpStatsSerializerSnapshot{version=" + version + "}";
    }
}
