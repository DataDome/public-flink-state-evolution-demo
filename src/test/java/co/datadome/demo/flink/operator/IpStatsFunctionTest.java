package co.datadome.demo.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;

import co.datadome.demo.flink.model.HttpRequest;
import co.datadome.demo.flink.model.IpStats;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IpStatsFunctionTest {

    private static final String IP = "10.0.0.1";

    private KeyedOneInputStreamOperatorTestHarness<String, HttpRequest, IpStats> harness;

    @BeforeEach
    void setUp() throws Exception {
        harness = newHarness();
        harness.open();
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, HttpRequest, IpStats> newHarness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new IpStatsFunction()),
                HttpRequest::getIp,
                Types.STRING);
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    private void send(long timestampMs, String ip, String path, int statusCode) throws Exception {
        harness.processElement(
                new StreamRecord<>(
                        new HttpRequest(timestampMs, ip, path, "curl/8", statusCode), timestampMs));
    }

    private List<IpStats> emitted() {
        return harness.extractOutputStreamRecords().stream()
                .map(record -> (IpStats) record.getValue())
                .toList();
    }

    @Test
    void emitsUpdatedStatisticsOnEveryRequest() throws Exception {
        send(1_000, IP, "/login", 200);
        send(2_000, IP, "/login", 200);
        send(3_000, IP, "/search", 200);

        assertThat(emitted()).hasSize(3);
        assertThat(emitted()).extracting(IpStats::getTotalCount).containsExactly(1L, 2L, 3L);
    }

    @Test
    void countsErrorsSeparately() throws Exception {
        send(1_000, IP, "/login", 200);
        send(2_000, IP, "/login", 403);
        send(3_000, IP, "/login", 500);

        IpStats last = emitted().getLast();
        assertThat(last.getTotalCount()).isEqualTo(3);
        assertThat(last.getErrorCount()).isEqualTo(2);
    }

    @Test
    void countsDistinctPathsOnlyOnce() throws Exception {
        send(1_000, IP, "/login", 200);
        send(2_000, IP, "/login", 200);
        send(3_000, IP, "/search", 200);
        send(4_000, IP, "/login", 200);

        assertThat(emitted()).extracting(IpStats::getDistinctPathCount).containsExactly(1, 1, 2, 2);
    }

    @Test
    void tracksSessionBoundsInEventTime() throws Exception {
        send(1_000, IP, "/a", 200);
        send(7_000, IP, "/b", 200);

        IpStats last = emitted().getLast();
        assertThat(last.getIp()).isEqualTo(IP);
        assertThat(last.getSessionStartMs()).isEqualTo(1_000);
        assertThat(last.getLastSeenMs()).isEqualTo(7_000);
    }

    @Test
    void keepsStatisticsSeparatePerIpAddress() throws Exception {
        send(1_000, "10.0.0.1", "/a", 200);
        send(2_000, "10.0.0.2", "/a", 200);
        send(3_000, "10.0.0.1", "/b", 200);

        assertThat(emitted()).extracting(IpStats::getIp, IpStats::getTotalCount)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("10.0.0.1", 1L),
                        org.assertj.core.groups.Tuple.tuple("10.0.0.2", 1L),
                        org.assertj.core.groups.Tuple.tuple("10.0.0.1", 2L));
    }

    @Test
    void discardsStateAfterTheInactivityGap() throws Exception {
        send(1_000, IP, "/a", 200);
        assertThat(harness.numKeyedStateEntries()).isPositive();

        harness.processWatermark(new Watermark(1_000 + SessionExpiry.GAP_MS + 1));

        assertThat(harness.numKeyedStateEntries())
                .as("all state for the IP address should be gone after the inactivity gap")
                .isZero();
    }

    @Test
    void keepsStateWhileTheSessionIsStillActive() throws Exception {
        send(1_000, IP, "/a", 200);
        // A later request keeps the session alive past the point the first timer was set for.
        send(SessionExpiry.GAP_MS, IP, "/b", 200);

        harness.processWatermark(new Watermark(1_000 + SessionExpiry.GAP_MS + 1));

        assertThat(harness.numKeyedStateEntries())
                .as("the session is still active, so the timer must have been re-registered")
                .isPositive();

        // Once the later activity has itself aged out, the state finally goes away.
        harness.processWatermark(new Watermark(2 * SessionExpiry.GAP_MS + 1));
        assertThat(harness.numKeyedStateEntries()).isZero();
    }

    @Test
    void startsAFreshSessionAfterExpiry() throws Exception {
        send(1_000, IP, "/a", 200);
        harness.processWatermark(new Watermark(1_000 + SessionExpiry.GAP_MS + 1));

        long later = 5 * SessionExpiry.GAP_MS;
        send(later, IP, "/a", 200);

        IpStats fresh = emitted().getLast();
        assertThat(fresh.getTotalCount()).isEqualTo(1);
        assertThat(fresh.getDistinctPathCount()).isEqualTo(1);
        assertThat(fresh.getSessionStartMs()).isEqualTo(later);
    }
    @Test
    void statisticsSurviveASnapshotAndRestore() throws Exception {
        send(1_000, IP, "/login", 403);
        send(2_000, IP, "/search", 200);

        OperatorSubtaskState snapshot = harness.snapshot(1L, 1L);
        harness.close();

        // A fresh operator, reading the state back through IpStatsSerializer.
        harness = newHarness();
        harness.initializeState(snapshot);
        harness.open();

        send(3_000, IP, "/login", 200);

        IpStats restored = emitted().getLast();
        assertThat(restored.getSessionStartMs())
                .as("the session continues rather than starting again")
                .isEqualTo(1_000);
        assertThat(restored.getTotalCount()).isEqualTo(3);
        assertThat(restored.getErrorCount()).isEqualTo(1);
        assertThat(restored.getDistinctPathCount()).isEqualTo(2);
    }
}
