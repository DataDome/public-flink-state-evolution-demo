package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.IpStats;
import java.io.IOException;

import lombok.EqualsAndHashCode;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Writes {@link IpStats} to and from Flink state by hand.
 *
 * <p>Flink serializes {@link IpStats} perfectly well on its own, so this class is not needed. It
 * exists to show what a custom serializer has to do to stay restorable across versions of a job,
 * which is the part {@code PojoSerializer} otherwise hides.
 */
public final class IpStatsSerializer extends TypeSerializer<IpStats> {

    private static final long serialVersionUID = 1L;

    /** Version of the record layout this code reads and writes. */
    public static final int LATEST_VERSION = 1;

    /**
     * Version of the record layout this instance handles: {@link #LATEST_VERSION}, or the version read
     * from the savepoint when this serializer was built to restore existing state.
     */
    private final int version;

    public IpStatsSerializer() {
        this(LATEST_VERSION);
    }

    IpStatsSerializer(int version) {
        this.version = version;
    }

    /** Version of the record layout this instance handles. */
    public int getVersion() {
        return version;
    }

    @Override
    public boolean isImmutableType() {
        // IpStats is updated in place by the operator.
        return false;
    }

    @Override
    public TypeSerializer<IpStats> duplicate() {
        // Immutable, so it is already safe to share between threads.
        return this;
    }

    @Override
    public IpStats createInstance() {
        return new IpStats();
    }

    @Override
    public IpStats copy(IpStats from) {
        return from.copy();
    }

    @Override
    public IpStats copy(IpStats from, IpStats reuse) {
        reuse.setIp(from.getIp());
        reuse.setSessionStartMs(from.getSessionStartMs());
        reuse.setLastSeenMs(from.getLastSeenMs());
        reuse.setTotalCount(from.getTotalCount());
        reuse.setErrorCount(from.getErrorCount());
        reuse.setDistinctPathCount(from.getDistinctPathCount());
        return reuse;
    }

    @Override
    public int getLength() {
        // Variable: the IP address is a string.
        return -1;
    }

    @Override
    public void serialize(IpStats record, DataOutputView target) throws IOException {
        StringSerializer.INSTANCE.serialize(record.getIp(), target);
        target.writeLong(record.getSessionStartMs());
        target.writeLong(record.getLastSeenMs());
        target.writeLong(record.getTotalCount());
        target.writeLong(record.getErrorCount());
        target.writeInt(record.getDistinctPathCount());
    }

    @Override
    public IpStats deserialize(DataInputView source) throws IOException {
        return deserialize(new IpStats(), source);
    }

    @Override
    public IpStats deserialize(IpStats reuse, DataInputView source) throws IOException {
        reuse.setIp(StringSerializer.INSTANCE.deserialize(source));
        reuse.setSessionStartMs(source.readLong());
        reuse.setLastSeenMs(source.readLong());
        reuse.setTotalCount(source.readLong());
        reuse.setErrorCount(source.readLong());
        reuse.setDistinctPathCount(source.readInt());
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        // Could be done byte by byte, but the record is small and this cannot drift from the layout.
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<IpStats> snapshotConfiguration() {
        return new IpStatsSerializerSnapshot(version);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof IpStatsSerializer that && version == that.version;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(version);
    }

    @Override
    public String toString() {
        return "IpStatsSerializer{version=" + version + "}";
    }
}
