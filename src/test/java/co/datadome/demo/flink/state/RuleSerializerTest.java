package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.Metric;
import co.datadome.demo.flink.model.Rule;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RuleSerializerTest {

    /** A layout version this job does not have, standing in for a future or foreign one. */
    private static final int UNKNOWN_VERSION = 99;

    private static Rule sample() {
        return new Rule("failing-a-lot", Metric.ERROR_RATIO_AT_LEAST, 0.5, 50, true);
    }

    private static byte[] write(TypeSerializer<Rule> serializer, Rule value) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    private static Rule read(TypeSerializer<Rule> serializer, byte[] bytes) throws IOException {
        return serializer.deserialize(new DataInputDeserializer(bytes));
    }

    @Test
    void recordSurvivesARoundTrip() throws Exception {
        RuleSerializer serializer = new RuleSerializer();

        assertThat(read(serializer, write(serializer, sample()))).isEqualTo(sample());
    }

    @Test
    void everyMetricSurvivesARoundTrip() throws Exception {
        RuleSerializer serializer = new RuleSerializer();
        for (Metric metric : Metric.values()) {
            Rule rule = sample();
            rule.setMetric(metric);

            assertThat(read(serializer, write(serializer, rule)).getMetric()).isEqualTo(metric);
        }
    }

    @Test
    void nullStringsSurviveARoundTrip() throws Exception {
        RuleSerializer serializer = new RuleSerializer();
        Rule rule = new Rule(null, null, 1.0, 0, false);

        assertThat(read(serializer, write(serializer, rule))).isEqualTo(rule);
    }

    @Test
    void anUnknownMetricFailsWithAnIoException() throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(64);
        StringSerializer.INSTANCE.serialize("r1", out);
        StringSerializer.INSTANCE.serialize("NOT_A_METRIC", out);
        out.writeDouble(1.0);
        out.writeLong(0);
        out.writeBoolean(true);

        assertThatThrownBy(() -> read(new RuleSerializer(), out.getCopyOfBuffer()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("NOT_A_METRIC");
    }

    @Test
    void reusingARecordDoesNotLeaveStaleValuesBehind() throws Exception {
        RuleSerializer serializer = new RuleSerializer();
        Rule reuse = sample();

        Rule other = new Rule("r2", Metric.DISTINCT_PATHS_AT_MOST, 2, 10, false);
        Rule result = serializer.deserialize(reuse, new DataInputDeserializer(write(serializer, other)));

        assertThat(result).isEqualTo(other);
    }

    @Test
    void copyPreservesEveryField() {
        assertThat(new RuleSerializer().copy(sample())).isEqualTo(sample()).isNotSameAs(sample());
        assertThat(new RuleSerializer().copy(sample(), new Rule())).isEqualTo(sample());
    }

    @Test
    void copyingThroughTheViewsPreservesTheBytes() throws Exception {
        RuleSerializer serializer = new RuleSerializer();
        byte[] original = write(serializer, sample());

        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.copy(new DataInputDeserializer(original), out);

        assertThat(out.getCopyOfBuffer()).isEqualTo(original);
    }

    @Test
    void aRestoredSerializerCarriesTheVersionItWasRestoredWith() {
        TypeSerializer<Rule> restored = new RuleSerializerSnapshot(UNKNOWN_VERSION).restoreSerializer();

        assertThat(((RuleSerializer) restored).getVersion()).isEqualTo(UNKNOWN_VERSION);
        assertThat(restored).isNotEqualTo(new RuleSerializer());
    }

    @Test
    void theLayoutVersionSurvivesBeingWrittenAndReadBack() throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(64);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, new RuleSerializer().snapshotConfiguration());

        TypeSerializerSnapshot<Rule> readBack = TypeSerializerSnapshot.readVersionedSnapshot(
                new DataInputDeserializer(out.getCopyOfBuffer()), getClass().getClassLoader());

        assertThat(readBack).isInstanceOf(RuleSerializerSnapshot.class);
        assertThat(readBack.getCurrentVersion()).isEqualTo(RuleSerializer.LATEST_VERSION);
        assertThat(readBack.restoreSerializer()).isEqualTo(new RuleSerializer());
    }

    @Test
    void theSameLayoutIsCompatibleAsIs() {
        assertThat(resolveAgainst(new RuleSerializer().snapshotConfiguration()).isCompatibleAsIs()).isTrue();
    }

    @Test
    void anotherLayoutVersionIsReadRatherThanFailingTheRead() throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(32);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, new RuleSerializerSnapshot(UNKNOWN_VERSION));

        TypeSerializerSnapshot<Rule> readBack = TypeSerializerSnapshot.readVersionedSnapshot(
                new DataInputDeserializer(out.getCopyOfBuffer()), getClass().getClassLoader());

        assertThat(readBack.getCurrentVersion()).isEqualTo(UNKNOWN_VERSION);
        assertThat(resolveAgainst(readBack).isIncompatible()).isTrue();
    }

    @Test
    void stateWrittenByThePojoSerializerIsCompatible() {
        TypeSerializerSnapshot<Rule> pojoSnapshot = TypeInformation.of(Rule.class)
                .createSerializer(new SerializerConfigImpl())
                .snapshotConfiguration();

        assertThat(resolveAgainst(pojoSnapshot).isIncompatible()).isFalse();
    }

    @Test
    void serializersAreEqualOnlyWhenTheirVersionMatches() {
        assertThat(new RuleSerializer())
                .isEqualTo(new RuleSerializer())
                .hasSameHashCodeAs(new RuleSerializer());
        assertThat(new RuleSerializer()).isNotEqualTo(new RuleSerializer(UNKNOWN_VERSION));
    }

    /** Resolves compatibility of state described by that snapshot against the current serializer. */
    private static TypeSerializerSchemaCompatibility<Rule> resolveAgainst(TypeSerializerSnapshot<Rule> oldSnapshot) {
        return new RuleSerializer().snapshotConfiguration().resolveSchemaCompatibility(oldSnapshot);
    }
}
