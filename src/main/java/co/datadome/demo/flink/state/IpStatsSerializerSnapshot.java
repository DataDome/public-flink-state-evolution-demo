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
 *
 * <p>Two versions meet here, and they are deliberately kept apart. {@link #getCurrentVersion()}
 * is the version of <em>this snapshot's own</em> format, which Flink writes and passes back to
 * {@link #readSnapshot}; it changes when the bytes below change. The layout version of
 * {@link IpStats} is written into those bytes as a payload of its own.
 */
public final class IpStatsSerializerSnapshot implements TypeSerializerSnapshot<IpStats> {

    /** Version of the layout of {@link #writeSnapshot}, i.e. of the bytes this class writes. */
    private static final int SNAPSHOT_VERSION = 1;

    /** State of a snapshot that has not read back IpStats version yet. */
    private static final int UNINITIALIZED_IP_STATS_VERSION = -1;

    /** Version of the IpStats record layout this snapshot describes. */
    private int ipStatsVersion;

    /** Required: Flink instantiates this reflectively before calling {@link #readSnapshot}. */
    public IpStatsSerializerSnapshot() {
        this(UNINITIALIZED_IP_STATS_VERSION);
    }

    IpStatsSerializerSnapshot(int ipStatsVersion) {
        this.ipStatsVersion = ipStatsVersion;
    }

    @Override
    public int getCurrentVersion() {
        return SNAPSHOT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        out.writeInt(ipStatsVersion);
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        if (readVersion != SNAPSHOT_VERSION) {
            // A savepoint written by a newer version of this class than the one running.
            throw new IOException("Unsupported snapshot version " + readVersion);
        }
        ipStatsVersion = in.readInt();
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

        if (that.ipStatsVersion == ipStatsVersion) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }

        // Only one layout exists so far. Once an older one does, it is recognized by its version
        // here and answered with compatibleAfterMigration().
        return TypeSerializerSchemaCompatibility.incompatible();
    }

    /** Builds a serializer that handles the layout this snapshot describes. */
    @Override
    public TypeSerializer<IpStats> restoreSerializer() {
        return new IpStatsSerializer(ipStatsVersion);
    }

    @Override
    public String toString() {
        return "IpStatsSerializerSnapshot{version=" + ipStatsVersion + "}";
    }

}
