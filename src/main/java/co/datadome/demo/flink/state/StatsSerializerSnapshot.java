package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.Stats;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * What Flink stores alongside the state so that it can work out, on restore, whether the state can
 * still be read.
 *
 * <p>Flink identifies this class by name in the savepoint, so it answers to two of them: see
 * {@link IpStatsSerializerSnapshot}, the name it had before {@code IpStats} was renamed.
 */
public final class StatsSerializerSnapshot implements TypeSerializerSnapshot<Stats> {

    /**
     * Version of the record layout this snapshot describes. Exposed through
     * {@link #getCurrentVersion()}, which is what gets persisted and read back.
     */
    private int version;

    /** Required: Flink instantiates this reflectively before calling {@link #readSnapshot}. */
    public StatsSerializerSnapshot() {
        this(StatsSerializer.LATEST_VERSION);
    }

    StatsSerializerSnapshot(int version) {
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
    public TypeSerializer<Stats> restoreSerializer() {
        return new StatsSerializer(version);
    }

    /**
     * Decides whether state described by that snapshot can be read by the serializer this snapshot
     * belongs to.
     *
     * <p>Note the direction: this is the snapshot of the serializer the job wants to use now, and
     * that argument is the one restored from the savepoint.
     */
    @Override
    public TypeSerializerSchemaCompatibility<Stats> resolveSchemaCompatibility(
            TypeSerializerSnapshot<Stats> old) {

        // Accept the name before the rename to let the savepoint be restored
        if (!(old instanceof StatsSerializerSnapshot || old instanceof IpStatsSerializerSnapshot)) {
            // The state was written by a different serializer altogether, PojoSerializer for
            // instance. Nothing here knows how to read it.
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        if (old.getCurrentVersion() == version) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }

        // Only one layout exists so far. Once an older one does, it is recognised by its version
        // here and answered with compatibleAfterMigration().
        return TypeSerializerSchemaCompatibility.incompatible();
    }

    @Override
    public String toString() {
        return "StatsSerializerSnapshot{version=" + version + "}";
    }
}
