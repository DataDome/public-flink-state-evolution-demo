package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.IpStats;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Writes {@link IpStats} to and from Flink state.
 * <p>
 * A wrapper for a PojoSerializer for the IpStats class. This is necessary to prepare migration to using only the
 * PojoSerializer.
 */
public final class IpStatsPojoSerializer extends TypeSerializer<IpStats> {

    private static final long serialVersionUID = 1L;

    static final PojoTypeInfo<IpStats> IP_STATS_POJO_TYPE_INFO = (PojoTypeInfo<IpStats>) Types.POJO(IpStats.class);

    private final SerializerConfig serializerConfig;

    private final PojoSerializer<IpStats> wrapped;

    private final IpStatsSerializer oldCustomSerializer = new IpStatsSerializer();

    /**
     * If not reconfigured, we don't generate a pojo serializer because we just want to produce the custom
     * IpStatsPojoSerializerSnapshot.
     */
    public IpStatsPojoSerializer(SerializerConfig serializerConfig, boolean reconfigure) {
        this.serializerConfig = serializerConfig;
        if (reconfigure) {
            this.wrapped = IP_STATS_POJO_TYPE_INFO.createPojoSerializer(serializerConfig);
        } else {
            this.wrapped = null; // no actual wrapped serializer, this instance is only here to provide a snapshot to resolve compatibility
        }
    }

    /**
     * Special case if we need to redeploy this version.
     */
    public IpStatsPojoSerializer(PojoSerializer<IpStats> pojoSerializer) {
        this.serializerConfig = null; // not needed, we already have a wrapped serializer
        this.wrapped = pojoSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return wrapped.isImmutableType();
    }

    @Override
    public TypeSerializer<IpStats> duplicate() {
        if (wrapped == null) {
            return this;
        } else {
            return new IpStatsPojoSerializer(wrapped.duplicate());
        }
    }

    @Override
    public IpStats createInstance() {
        return wrapped.createInstance();
    }

    @Override
    public IpStats copy(IpStats from) {
        return wrapped.copy(from);
    }

    @Override
    public IpStats copy(IpStats from, IpStats reuse) {
        return wrapped.copy(from, reuse);
    }

    @Override
    public int getLength() {
        return wrapped.getLength();
    }

    @Override
    public void serialize(IpStats record, DataOutputView target) throws IOException {
        wrapped.serialize(record, target);
    }

    /** When deserializing from the savepoint, we are reading the old layout. */
    @Override
    public IpStats deserialize(DataInputView source) throws IOException {
        return oldCustomSerializer.deserialize(source);
    }

    @Override
    public IpStats deserialize(IpStats reuse, DataInputView source) throws IOException {
        return oldCustomSerializer.deserialize(reuse, source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    /**
     * The registered TS returns a snapshot that confirms compatibility with the old IpStatsSerializerSnapshot.
     * Once reconfigured, it returns a basic PojoSerializerSnapshot, so that ulterior versions can work with it.
     */
    @Override
    public TypeSerializerSnapshot<IpStats> snapshotConfiguration() {
        if (wrapped != null) {
            return wrapped.snapshotConfiguration();
        } else {
            return new IpStatsPojoSerializerSnapshot(serializerConfig);
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof IpStatsPojoSerializer that && wrapped.equals(that.wrapped);
    }

    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }

    @Override
    public String toString() {
        return "IpStatsPojoSerializer{}";
    }
}
