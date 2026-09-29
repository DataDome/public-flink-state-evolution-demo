package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.IpStats;
import lombok.Getter;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import co.datadome.demo.flink.model.IpStats;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * This snapshot will only be used to resolve compatibility with the previous, non-pojo snapshot. It will never
 * get serialized into a savepoint, because the IpStatsPojoSerializer used to serialize into the snapshot produces
 * a PojoSerializer.
 */
public class IpStatsPojoSerializerSnapshot implements TypeSerializerSnapshot<IpStats> {

    /** Not actually serialized, needed to provide the reconfigured serializer */
    private final SerializerConfig serializerConfig;

    // no-arg constructor is never used as this snapshot is never serialized

    IpStatsPojoSerializerSnapshot( SerializerConfig serializerConfig) {
        this.serializerConfig = serializerConfig;
    }

    @Override
    public int getCurrentVersion() {
        return 0;
    }

    @Override
    public void writeSnapshot(DataOutputView out) {
        throw new UnsupportedOperationException(); // never serialized
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) {
        throw new UnsupportedOperationException(); // never serialized
    }

    @Override
    public TypeSerializer<IpStats> restoreSerializer() {
        throw new UnsupportedOperationException(); // never used
    }

    /**
     * With
     */
    @Override
    public TypeSerializerSchemaCompatibility<IpStats> resolveSchemaCompatibility(TypeSerializerSnapshot<IpStats> old) {

        if (old instanceof IpStatsSerializerSnapshot that) {
            return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                    new IpStatsPojoSerializer(serializerConfig, true)
            );
        }

        // Only happens if we redeploy this version of the code
        if (old instanceof PojoSerializerSnapshot<IpStats> that) {
            return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                    new IpStatsPojoSerializer((PojoSerializer<IpStats>) that.restoreSerializer())
            );
        }

        // The state was written by a serializer we don't recognize, and can't read.
        return TypeSerializerSchemaCompatibility.incompatible();
    }

    @Override
    public String toString() {
        return "IpStatsSerializerSnapshot{}";
    }
}
