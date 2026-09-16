package co.datadome.demo.flink.state;

import co.datadome.demo.flink.model.IpStats;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class IpStatsSerializerTest {

    /** A layout version this job does not have, standing in for a future or foreign one. */
    private static final int UNKNOWN_VERSION = 99;

    private static IpStats sample() {
        return IpStats.builder()
                .ip("10.0.0.66")
                .sessionStartMs(1_000)
                .lastSeenMs(9_000)
                .totalCount(500)
                .errorCount(400)
                .distinctPathCount(3)
                .build();
    }

    private static byte[] write(TypeSerializer<IpStats> serializer, IpStats value)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.serialize(value, out);
        return out.getCopyOfBuffer();
    }

    private static IpStats read(TypeSerializer<IpStats> serializer, byte[] bytes)
            throws IOException {
        return serializer.deserialize(new DataInputDeserializer(bytes));
    }

    @Test
    void recordSurvivesARoundTrip() throws Exception {
        IpStatsSerializer serializer = new IpStatsSerializer();

        assertThat(read(serializer, write(serializer, sample()))).isEqualTo(sample());
    }

    @Test
    void aNullIpAddressSurvivesARoundTrip() throws Exception {
        IpStatsSerializer serializer = new IpStatsSerializer();
        IpStats stats = sample();
        stats.setIp(null);

        assertThat(read(serializer, write(serializer, stats)).getIp()).isNull();
    }

    @Test
    void reusingARecordDoesNotLeaveStaleValuesBehind() throws Exception {
        IpStatsSerializer serializer = new IpStatsSerializer();
        IpStats reuse = sample();

        IpStats other = IpStats.builder().ip("10.0.0.1").totalCount(1).build();
        IpStats result =
                serializer.deserialize(reuse, new DataInputDeserializer(write(serializer, other)));

        assertThat(result).isEqualTo(other);
    }

    @Test
    void copyPreservesEveryField() {
        assertThat(new IpStatsSerializer().copy(sample())).isEqualTo(sample());
        assertThat(new IpStatsSerializer().copy(sample(), new IpStats())).isEqualTo(sample());
    }

    @Test
    void copyingThroughTheViewsPreservesTheBytes() throws Exception {
        IpStatsSerializer serializer = new IpStatsSerializer();
        byte[] original = write(serializer, sample());

        DataOutputSerializer out = new DataOutputSerializer(64);
        serializer.copy(new DataInputDeserializer(original), out);

        assertThat(out.getCopyOfBuffer()).isEqualTo(original);
    }

    @Test
    void theSnapshotReportsTheSerializersVersion() {
        assertThat(new IpStatsSerializer().snapshotConfiguration().getCurrentVersion())
                .isEqualTo(IpStatsSerializer.LATEST_VERSION);
        assertThat(new IpStatsSerializerSnapshot(UNKNOWN_VERSION).getCurrentVersion())
                .isEqualTo(UNKNOWN_VERSION);
    }

    @Test
    void aRestoredSerializerCarriesTheVersionItWasRestoredWith() {
        // The version comes from the savepoint, not from this code: that is what lets a future
        // layout tell state written today apart from state it writes itself.
        assertThat(new IpStatsSerializer().getVersion()).isEqualTo(IpStatsSerializer.LATEST_VERSION);

        TypeSerializer<IpStats> restored =
                new IpStatsSerializerSnapshot(UNKNOWN_VERSION).restoreSerializer();

        assertThat(((IpStatsSerializer) restored).getVersion()).isEqualTo(UNKNOWN_VERSION);
        assertThat(restored)
                .as("a serializer for another layout is not the current one")
                .isNotEqualTo(new IpStatsSerializer());
    }

    @Test
    void theLayoutVersionSurvivesBeingWrittenAndReadBack() throws Exception {
        // The version is the only thing preparing this serializer for a future layout, so it has
        // to come back out of the snapshot intact.
        DataOutputSerializer out = new DataOutputSerializer(64);
        TypeSerializerSnapshot.writeVersionedSnapshot(
                out, new IpStatsSerializer().snapshotConfiguration());

        TypeSerializerSnapshot<IpStats> readBack =
                TypeSerializerSnapshot.readVersionedSnapshot(
                        new DataInputDeserializer(out.getCopyOfBuffer()),
                        getClass().getClassLoader());

        assertThat(readBack).isInstanceOf(IpStatsSerializerSnapshot.class);
        assertThat(readBack.getCurrentVersion()).isEqualTo(IpStatsSerializer.LATEST_VERSION);
        assertThat(readBack.restoreSerializer()).isEqualTo(new IpStatsSerializer());
    }

    @Test
    void aRecordWrittenWithThePreviousLayoutSurvivesARoundTrip() throws Exception {
        // The restored serializer is the only thing that can read a version 1 record, and it has to
        // write it back in the same layout so that copy() through the views stays byte for byte.
        IpStatsSerializer previous = new IpStatsSerializer(IpStatsSerializer.PREVIOUS_VERSION);

        assertThat(read(previous, write(previous, sample()))).isEqualTo(sample());
    }

    @Test
    void thePreviousLayoutSpendsFourMoreBytesOnTheErrorCount() throws Exception {
        // Nothing else moved, so the whole difference between the two layouts is errorCount going
        // from a long to an int.
        byte[] previous = write(new IpStatsSerializer(IpStatsSerializer.PREVIOUS_VERSION), sample());

        assertThat(write(new IpStatsSerializer(), sample())).hasSize(previous.length - 4);
    }

    @Test
    void thePreviousLayoutIsCompatibleAfterMigration() {
        // Flink reads the old records with restoreSerializer() and writes them back with the
        // current one, which is what rewrites the error counts as ints.
        assertThat(resolveAgainst(
                                new IpStatsSerializerSnapshot(IpStatsSerializer.PREVIOUS_VERSION))
                        .isCompatibleAfterMigration())
                .isTrue();
    }

    @Test
    void theCurrentLayoutIsNotReadableByThePreviousOne() {
        // Downgrading a job is not a migration Flink offers, and the shorter record proves why.
        assertThat(new IpStatsSerializerSnapshot(IpStatsSerializer.PREVIOUS_VERSION)
                        .resolveSchemaCompatibility(new IpStatsSerializer().snapshotConfiguration())
                        .isIncompatible())
                .isTrue();
    }

    @Test
    void theSameLayoutIsCompatibleAsIs() {
        assertThat(resolveAgainst(new IpStatsSerializer().snapshotConfiguration())
                        .isCompatibleAsIs())
                .isTrue();
    }

    @Test
    void anotherLayoutVersionIsIncompatible() {
        assertThat(resolveAgainst(new IpStatsSerializerSnapshot(UNKNOWN_VERSION)).isIncompatible())
                .isTrue();
    }

    @Test
    void anotherLayoutVersionIsReadRatherThanFailingTheRead() throws Exception {
        // State written with a layout this job does not know still has to be readable as metadata,
        // so that the incompatibility is reported rather than throwing while reading the snapshot.
        DataOutputSerializer out = new DataOutputSerializer(32);
        TypeSerializerSnapshot.writeVersionedSnapshot(
                out, new IpStatsSerializerSnapshot(UNKNOWN_VERSION));

        TypeSerializerSnapshot<IpStats> readBack =
                TypeSerializerSnapshot.readVersionedSnapshot(
                        new DataInputDeserializer(out.getCopyOfBuffer()),
                        getClass().getClassLoader());

        assertThat(readBack.getCurrentVersion()).isEqualTo(UNKNOWN_VERSION);
        assertThat(resolveAgainst(readBack).isIncompatible()).isTrue();
    }

    @Test
    void stateWrittenByThePojoSerializerIsIncompatible() {
        // This is what happens to a savepoint taken before the custom serializer was introduced.
        TypeSerializerSnapshot<IpStats> pojoSnapshot =
                TypeInformation.of(IpStats.class)
                        .createSerializer(new SerializerConfigImpl())
                        .snapshotConfiguration();

        assertThat(resolveAgainst(pojoSnapshot).isIncompatible()).isTrue();
    }

    @Test
    void serializersAreEqualOnlyWhenTheirVersionMatches() {
        assertThat(new IpStatsSerializer())
                .isEqualTo(new IpStatsSerializer())
                .hasSameHashCodeAs(new IpStatsSerializer());
        assertThat(new IpStatsSerializer()).isNotEqualTo(new IpStatsSerializer(UNKNOWN_VERSION));
    }

    @Test
    void theRecordLengthIsVariable() {
        assertThat(new IpStatsSerializer().getLength()).isEqualTo(-1);
    }

    /** Resolves compatibility of state described by that snapshot against the current serializer. */
    private static org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility<IpStats>
            resolveAgainst(TypeSerializerSnapshot<IpStats> oldSnapshot) {
        return new IpStatsSerializer().snapshotConfiguration().resolveSchemaCompatibility(oldSnapshot);
    }
}
