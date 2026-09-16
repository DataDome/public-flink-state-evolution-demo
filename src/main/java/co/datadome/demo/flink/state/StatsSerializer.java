package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.Stats;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Writes {@link Stats} to and from Flink state.
 */
public final class StatsSerializer extends TypeSerializer<Stats> {

    private static final long serialVersionUID = 1L;

    /**
     * Latest version of the serialized layout.
     */
    public static final int LATEST_VERSION = 1;

    /**
     * Version of the serialized layout this instance handles: {@link #LATEST_VERSION}, or the version read
     * from the savepoint when this serializer was built to restore existing state.
     */
    private final int version;

    public StatsSerializer() {
        this(LATEST_VERSION);
    }

    StatsSerializer(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public boolean isImmutableType() {
        // Stats is updated in place by the operator.
        return false;
    }

    @Override
    public TypeSerializer<Stats> duplicate() {
        // The type-serializer is immutable, so it is safe to share between threads.
        return this;
    }

    @Override
    public Stats createInstance() {
        return new Stats();
    }

    @Override
    public Stats copy(Stats from) {
        return from.copy();
    }

    @Override
    public Stats copy(Stats from, Stats reuse) {
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
    public void serialize(Stats record, DataOutputView target) throws IOException {
        StringSerializer.INSTANCE.serialize(record.getIp(), target);
        target.writeLong(record.getSessionStartMs());
        target.writeLong(record.getLastSeenMs());
        target.writeLong(record.getTotalCount());
        target.writeLong(record.getErrorCount());
        target.writeInt(record.getDistinctPathCount());
    }

    @Override
    public Stats deserialize(DataInputView source) throws IOException {
        return deserialize(new Stats(), source);
    }

    @Override
    public Stats deserialize(Stats reuse, DataInputView source) throws IOException {
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
        // Could be done byte by byte to be faster, which requires to know how many bytes must be read/written, which is
        // a consequence of the layout. Here, we went for the simpler (but slower) solution.
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<Stats> snapshotConfiguration() {
        return new StatsSerializerSnapshot(version);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof StatsSerializer that && version == that.version;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(version);
    }

    @Override
    public String toString() {
        return "StatsSerializer{version=" + version + "}";
    }
}
