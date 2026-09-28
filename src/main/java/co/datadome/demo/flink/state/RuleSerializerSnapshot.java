package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.Rule;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Serialized state of the type-serializer itself.
 */
public final class RuleSerializerSnapshot implements TypeSerializerSnapshot<Rule> {

    /**
     * Version of the serialized layout this snapshot describes, passed by the {@link RuleSerializer}, or read from
     * the savepoint when restoring the state.
     */
    private int version;

    /**
     * Required on deserialization: Flink instantiates this reflectively before calling {@link #readSnapshot}.
     */
    public RuleSerializerSnapshot() {
        this(RuleSerializer.LATEST_VERSION);
    }

    /**
     * Used in the type-serializer to create the snapshot.
     */
    RuleSerializerSnapshot(int version) {
        this.version = version;
    }

    @Override
    public int getCurrentVersion() {
        return version;
    }

    @Override
    public void writeSnapshot(DataOutputView out) {
        // Nothing to do: RuleSerializer doesn't have any state apart from the version, directly handled by Flink.
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) {
        // An unknown version is accepted on purpose, so that resolveSchemaCompatibility can
        // report it rather than the restore failing here.
        version = readVersion;
    }

    @Override
    public TypeSerializer<Rule> restoreSerializer() {
        return new RuleSerializer(version);
    }

    /**
     * Decides whether state described by that snapshot can be read by the serializer this snapshot
     * belongs to.
     *
     * <p>Note the direction: this is the snapshot of the serializer the job wants to use now, and
     * that argument is the one restored from the savepoint.
     */
    @Override
    public TypeSerializerSchemaCompatibility<Rule> resolveSchemaCompatibility(TypeSerializerSnapshot<Rule> old) {

        if (old instanceof RuleSerializerSnapshot that) {
            if (that.version == version) {
                return TypeSerializerSchemaCompatibility.compatibleAsIs();
            } else {
                // Only one layout is known, so any other version is incompatible
                return TypeSerializerSchemaCompatibility.incompatible();
            }
        }

        // Accept the PojoSerializer as well!
        if (old instanceof PojoSerializerSnapshot<?> that) {
            // We don't care about the PojoSerializer's version, let's consider it compatible
            // Force deserialization with the PojoSerializer, in case the layout differs from ours
            return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
        }

        // The state was written by a serializer we don't recognize, and can't read.
        return TypeSerializerSchemaCompatibility.incompatible();
    }

    @Override
    public String toString() {
        return "RuleSerializerSnapshot{version=" + version + "}";
    }
}
