package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.Stats;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class StatsSerializerTest {

    /** A layout version this job does not have, standing in for a future or foreign one. */
    private static final int UNKNOWN_VERSION = 99;

    private static Stats sample() {
        return Stats.builder()
                .ip("10.0.0.66")
                .sessionStartMs(1_000)
                .lastSeenMs(9_000)
                .totalCount(500)
                .errorCount(400)
                .distinctPathCount(3)
                .build();
    }

    private static byte[] write(TypeSerializer<Stats> serializer, Stats value)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    private static Stats read(TypeSerializer<Stats> serializer, byte[] bytes)
            throws IOException {
        return serializer.deserialize(new DataInputDeserializer(bytes));
    }

    @Test
    void recordSurvivesARoundTrip() throws Exception {
        StatsSerializer serializer = new StatsSerializer();

        assertThat(read(serializer, write(serializer, sample()))).isEqualTo(sample());
    }

    @Test
    void aNullIpAddressSurvivesARoundTrip() throws Exception {
        StatsSerializer serializer = new StatsSerializer();
        Stats stats = sample();
        stats.setIp(null);

        assertThat(read(serializer, write(serializer, stats)).getIp()).isNull();
    }

    @Test
    void reusingARecordDoesNotLeaveStaleValuesBehind() throws Exception {
        StatsSerializer serializer = new StatsSerializer();
        Stats reuse = sample();

        Stats other = Stats.builder().ip("10.0.0.1").totalCount(1).build();
        Stats result =
                serializer.deserialize(reuse, new DataInputDeserializer(write(serializer, other)));

        assertThat(result).isEqualTo(other);
    }

    @Test
    void copyPreservesEveryField() {
        assertThat(new StatsSerializer().copy(sample())).isEqualTo(sample());
        assertThat(new StatsSerializer().copy(sample(), new Stats())).isEqualTo(sample());
    }

    @Test
    void copyingThroughTheViewsPreservesTheBytes() throws Exception {
        StatsSerializer serializer = new StatsSerializer();
        byte[] original = write(serializer, sample());

        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.copy(new DataInputDeserializer(original), out);

        assertThat(out.getCopyOfBuffer()).isEqualTo(original);
    }

    @Test
    void theSnapshotReportsTheSerializersVersion() {
        assertThat(new StatsSerializer().snapshotConfiguration().getCurrentVersion())
                .isEqualTo(StatsSerializer.LATEST_VERSION);
        assertThat(new StatsSerializerSnapshot(UNKNOWN_VERSION).getCurrentVersion())
                .isEqualTo(UNKNOWN_VERSION);
    }

    @Test
    void aRestoredSerializerCarriesTheVersionItWasRestoredWith() {
        // The version comes from the savepoint, not from this code: that is what lets a future
        // layout tell state written today apart from state it writes itself.
        assertThat(new StatsSerializer().getVersion()).isEqualTo(StatsSerializer.LATEST_VERSION);

        TypeSerializer<Stats> restored =
                new StatsSerializerSnapshot(UNKNOWN_VERSION).restoreSerializer();

        assertThat(((StatsSerializer) restored).getVersion()).isEqualTo(UNKNOWN_VERSION);
        assertThat(restored)
                .as("a serializer for another layout is not the current one")
                .isNotEqualTo(new StatsSerializer());
    }

    @Test
    void theLayoutVersionSurvivesBeingWrittenAndReadBack() throws Exception {
        // The version is the only thing preparing this serializer for a future layout, so it has
        // to come back out of the snapshot intact.
        DataOutputSerializer out = new DataOutputSerializer(64);
        TypeSerializerSnapshot.writeVersionedSnapshot(
                out, new StatsSerializer().snapshotConfiguration());

        TypeSerializerSnapshot<Stats> readBack =
                TypeSerializerSnapshot.readVersionedSnapshot(
                        new DataInputDeserializer(out.getCopyOfBuffer()),
                        getClass().getClassLoader());

        assertThat(readBack).isInstanceOf(StatsSerializerSnapshot.class);
        assertThat(readBack.getCurrentVersion()).isEqualTo(StatsSerializer.LATEST_VERSION);
        assertThat(readBack.restoreSerializer()).isEqualTo(new StatsSerializer());
    }

    @Test
    void theSameLayoutIsCompatibleAsIs() {
        assertThat(resolveAgainst(new StatsSerializer().snapshotConfiguration())
                        .isCompatibleAsIs())
                .isTrue();
    }

    @Test
    void anotherLayoutVersionIsIncompatible() {
        assertThat(resolveAgainst(new StatsSerializerSnapshot(UNKNOWN_VERSION)).isIncompatible())
                .isTrue();
    }

    @Test
    void anotherLayoutVersionIsReadRatherThanFailingTheRead() throws Exception {
        // State written with a layout this job does not know still has to be readable as metadata,
        // so that the incompatibility is reported rather than throwing while reading the snapshot.
        DataOutputSerializer out = new DataOutputSerializer(32);
        TypeSerializerSnapshot.writeVersionedSnapshot(
                out, new StatsSerializerSnapshot(UNKNOWN_VERSION));

        TypeSerializerSnapshot<Stats> readBack =
                TypeSerializerSnapshot.readVersionedSnapshot(
                        new DataInputDeserializer(out.getCopyOfBuffer()),
                        getClass().getClassLoader());

        assertThat(readBack.getCurrentVersion()).isEqualTo(UNKNOWN_VERSION);
        assertThat(resolveAgainst(readBack).isIncompatible()).isTrue();
    }

    @Test
    void stateWrittenBeforeTheRenameIsStillRestorable() throws Exception {
        // The whole point of keeping IpStatsSerializerSnapshot: a savepoint taken before the rename
        // names that class, and both steps of the restore have to get past it.
        DataOutputSerializer out = new DataOutputSerializer(64);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, new IpStatsSerializerSnapshot());

        TypeSerializerSnapshot<Stats> readBack =
                TypeSerializerSnapshot.readVersionedSnapshot(
                        new DataInputDeserializer(out.getCopyOfBuffer()),
                        getClass().getClassLoader());

        assertThat(readBack)
                .as("the class named in the savepoint is still on the classpath")
                .isInstanceOf(IpStatsSerializerSnapshot.class);
        assertThat(resolveAgainst(readBack).isCompatibleAsIs())
                .as("and the current snapshot recognises it, so the state is read as it stands")
                .isTrue();
    }

    @Test
    void theSnapshotFromBeforeTheRenameHandsBackTheRenamedSerializer() {
        // Nothing records the serializer's own name, so it was free to be renamed with the record.
        assertThat(new IpStatsSerializerSnapshot(UNKNOWN_VERSION).restoreSerializer())
                .isEqualTo(new StatsSerializer(UNKNOWN_VERSION));
    }

    @Test
    void theSnapshotFromBeforeTheRenameIsStillHeldToTheLayoutVersion() {
        // Answering to the old name does not make it a free pass: the layout is checked the same
        // way it is for the current name.
        assertThat(resolveAgainst(new IpStatsSerializerSnapshot(UNKNOWN_VERSION)).isIncompatible())
                .isTrue();
    }

    @Test
    void stateWrittenByThePojoSerializerIsIncompatible() {
        // This is what happens to a savepoint taken before the custom serializer was introduced.
        TypeSerializerSnapshot<Stats> pojoSnapshot =
                TypeInformation.of(Stats.class)
                        .createSerializer(new SerializerConfigImpl())
                        .snapshotConfiguration();

        assertThat(resolveAgainst(pojoSnapshot).isIncompatible()).isTrue();
    }

    @Test
    void serializersAreEqualOnlyWhenTheirVersionMatches() {
        assertThat(new StatsSerializer())
                .isEqualTo(new StatsSerializer())
                .hasSameHashCodeAs(new StatsSerializer());
        assertThat(new StatsSerializer()).isNotEqualTo(new StatsSerializer(UNKNOWN_VERSION));
    }

    @Test
    void theRecordLengthIsVariable() {
        assertThat(new StatsSerializer().getLength()).isEqualTo(-1);
    }

    /** Resolves compatibility of state described by that snapshot against the current serializer. */
    private static org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility<Stats>
            resolveAgainst(TypeSerializerSnapshot<Stats> oldSnapshot) {
        return new StatsSerializer().snapshotConfiguration().resolveSchemaCompatibility(oldSnapshot);
    }
}
