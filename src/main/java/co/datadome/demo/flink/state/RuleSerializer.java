package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.Metric;
import co.datadome.demo.flink.model.Rule;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Writes {@link Rule} to and from Flink state.
 *
 * <p>The {@link Metric} is written by name, so adding or reordering constants is a compatible change, while renaming
 * or removing one is not.
 */
public final class RuleSerializer extends TypeSerializer<Rule> {

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

    public RuleSerializer() {
        this(LATEST_VERSION);
    }

    RuleSerializer(int version) {
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    @Override
    public boolean isImmutableType() {
        // Rule has setters.
        return false;
    }

    @Override
    public TypeSerializer<Rule> duplicate() {
        // The type-serializer is immutable, so it is safe to share between threads.
        return this;
    }

    @Override
    public Rule createInstance() {
        return new Rule();
    }

    @Override
    public Rule copy(Rule from) {
        return copy(from, new Rule());
    }

    @Override
    public Rule copy(Rule from, Rule reuse) {
        reuse.setRuleId(from.getRuleId());
        reuse.setMetric(from.getMetric());
        reuse.setThreshold(from.getThreshold());
        reuse.setMinTotalRequests(from.getMinTotalRequests());
        reuse.setEnabled(from.isEnabled());
        return reuse;
    }

    @Override
    public int getLength() {
        // Variable: the rule id and the metric are strings.
        return -1;
    }

    @Override
    public void serialize(Rule record, DataOutputView target) throws IOException {
        StringSerializer.INSTANCE.serialize(record.getRuleId(), target);
        StringSerializer.INSTANCE.serialize(record.getMetric() == null ? null : record.getMetric().name(), target);
        target.writeDouble(record.getThreshold());
        target.writeLong(record.getMinTotalRequests());
        target.writeBoolean(record.isEnabled());
    }

    @Override
    public Rule deserialize(DataInputView source) throws IOException {
        return deserialize(new Rule(), source);
    }

    @Override
    public Rule deserialize(Rule reuse, DataInputView source) throws IOException {
        reuse.setRuleId(StringSerializer.INSTANCE.deserialize(source));
        reuse.setMetric(readMetric(source));
        reuse.setThreshold(source.readDouble());
        reuse.setMinTotalRequests(source.readLong());
        reuse.setEnabled(source.readBoolean());
        return reuse;
    }

    private static Metric readMetric(DataInputView source) throws IOException {
        String name = StringSerializer.INSTANCE.deserialize(source);
        if (name == null) {
            return null;
        }
        try {
            return Metric.valueOf(name);
        } catch (IllegalArgumentException e) {
            throw new IOException("Unknown metric in state: " + name, e);
        }
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        // Could be done byte by byte to be faster, which requires to know how many bytes must be read/written, which is
        // a consequence of the layout. Here, we went for the simpler (but slower) solution.
        serialize(deserialize(source), target);
    }

    @Override
    public TypeSerializerSnapshot<Rule> snapshotConfiguration() {
        return new RuleSerializerSnapshot(version);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RuleSerializer that && version == that.version;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(version);
    }

    @Override
    public String toString() {
        return "RuleSerializer{version=" + version + "}";
    }
}
