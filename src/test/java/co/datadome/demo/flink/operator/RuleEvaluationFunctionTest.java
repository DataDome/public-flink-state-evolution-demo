package co.datadome.demo.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;

import co.datadome.demo.flink.model.HttpRequest;
import co.datadome.demo.flink.model.IpStats;
import co.datadome.demo.flink.model.Metric;
import co.datadome.demo.flink.model.Rule;
import co.datadome.demo.flink.model.RuleMatch;
import java.util.List;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RuleEvaluationFunctionTest {

    private static final String IP = "10.0.0.1";

    private KeyedBroadcastOperatorTestHarness<String, IpStats, Rule, RuleMatch> harness;

    @BeforeEach
    void setUp() throws Exception {
        CoBroadcastWithKeyedOperator<String, IpStats, Rule, RuleMatch> operator =
                new CoBroadcastWithKeyedOperator<>(
                        new RuleEvaluationFunction(),
                        List.<MapStateDescriptor<?, ?>>of(RuleEvaluationFunction.RULES_DESCRIPTOR));
        harness =
                new KeyedBroadcastOperatorTestHarness<>(
                        operator, IpStats::getIp, Types.STRING, 128, 1, 0);
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    /** Builds statistics for that IP address with that many total requests. */
    private static IpStats stats(String ip, long sessionStartMs, long totalCount) {
        IpStats stats = IpStats.startingWith(new HttpRequest(sessionStartMs, ip, "/a", "curl/8", 200));
        stats.setTotalCount(totalCount);
        stats.setLastSeenMs(sessionStartMs + totalCount);
        return stats;
    }

    /** Builds statistics with that error ratio, expressed as a count of failures out of the total. */
    private static IpStats statsWithErrors(long totalCount, long errorCount) {
        IpStats stats = stats(IP, 1_000, totalCount);
        stats.setErrorCount(errorCount);
        return stats;
    }

    /** Builds statistics with that many distinct paths. */
    private static IpStats statsWithPaths(long totalCount, int distinctPathCount) {
        IpStats stats = stats(IP, 1_000, totalCount);
        stats.setDistinctPathCount(distinctPathCount);
        return stats;
    }

    private void sendRule(Rule rule) throws Exception {
        harness.processBroadcastElement(new StreamRecord<>(rule, 0L));
    }

    private void sendStats(IpStats stats) throws Exception {
        harness.processElement(new StreamRecord<>(stats, stats.getLastSeenMs()));
    }

    private List<RuleMatch> matches() {
        return harness.extractOutputStreamRecords().stream()
                .map(record -> (RuleMatch) record.getValue())
                .toList();
    }

    @Test
    void doesNotFireBelowTheThreshold() throws Exception {
        sendRule(new Rule("r1", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, true));
        sendStats(stats(IP, 1_000, 9));

        assertThat(matches()).isEmpty();
    }

    @Test
    void firesWhenTheThresholdIsReached() throws Exception {
        sendRule(new Rule("r1", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, true));
        sendStats(stats(IP, 1_000, 10));

        assertThat(matches()).singleElement().satisfies(match -> {
            assertThat(match.getRuleId()).isEqualTo("r1");
            assertThat(match.getIp()).isEqualTo(IP);
            assertThat(match.getMetric()).isEqualTo(Metric.TOTAL_REQUESTS_AT_LEAST);
            assertThat(match.getObservedValue()).isEqualTo(10.0);
            assertThat(match.getThreshold()).isEqualTo(10.0);
        });
    }

    @Test
    void firesOnlyOncePerSession() throws Exception {
        sendRule(new Rule("r1", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, true));
        sendStats(stats(IP, 1_000, 10));
        sendStats(stats(IP, 1_000, 11));
        sendStats(stats(IP, 1_000, 50));

        assertThat(matches()).hasSize(1);
    }

    @Test
    void firesAgainForANewSession() throws Exception {
        sendRule(new Rule("r1", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, true));
        sendStats(stats(IP, 1_000, 10));
        // A new session for the same IP address: sessionStartMs moved forward.
        sendStats(stats(IP, 9_000_000, 10));

        assertThat(matches()).hasSize(2);
    }

    @Test
    void ignoresADisabledRule() throws Exception {
        sendRule(new Rule("r1", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, true));
        sendRule(new Rule("r1", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, false));
        sendStats(stats(IP, 1_000, 50));

        assertThat(matches()).isEmpty();
    }

    @Test
    void appliesTheLatestVersionOfARule() throws Exception {
        sendRule(new Rule("r1", Metric.TOTAL_REQUESTS_AT_LEAST, 100, 0, true));
        sendStats(stats(IP, 1_000, 50));
        assertThat(matches()).isEmpty();

        sendRule(new Rule("r1", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, true));
        sendStats(stats(IP, 1_000, 51));

        assertThat(matches()).singleElement()
                .extracting(RuleMatch::getThreshold)
                .isEqualTo(10.0);
    }

    @Test
    void republishingARuleLetsItFireAgainInTheSameSession() throws Exception {
        sendRule(new Rule("r1", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, true));
        sendStats(stats(IP, 1_000, 10));
        assertThat(matches()).hasSize(1);

        // Same rule published again: the operator must forget that it already fired.
        sendRule(new Rule("r1", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, true));
        sendStats(stats(IP, 1_000, 11));

        assertThat(matches()).hasSize(2);
    }

    @Test
    void evaluatesEveryRuleIndependently() throws Exception {
        sendRule(new Rule("total", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, true));
        sendRule(new Rule("errors", Metric.ERROR_RATIO_AT_LEAST, 0.5, 0, true));

        sendStats(statsWithErrors(10, 5));

        assertThat(matches()).extracting(RuleMatch::getRuleId)
                .containsExactlyInAnyOrder("total", "errors");
    }

    @Test
    void keepsRuleStateSeparatePerIpAddress() throws Exception {
        sendRule(new Rule("r1", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, true));
        sendStats(stats("10.0.0.1", 1_000, 10));
        sendStats(stats("10.0.0.2", 1_000, 10));

        assertThat(matches()).extracting(RuleMatch::getIp)
                .containsExactlyInAnyOrder("10.0.0.1", "10.0.0.2");
    }

    @Test
    void discardsStateAfterTheInactivityGap() throws Exception {
        sendRule(new Rule("r1", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, true));
        sendStats(stats(IP, 1_000, 10));
        assertThat(matches()).hasSize(1);
        assertThat(harness.numEventTimeTimers()).isPositive();

        // A two-input operator advances event time at the pace of its slowest input, so both sides
        // have to move for the expiry timer to fire.
        advanceBothInputsTo(10 * SessionExpiry.GAP_MS);

        assertThat(harness.numEventTimeTimers())
                .as("the expiry timer should not have been re-registered")
                .isZero();

        // The operator has forgotten this IP address entirely, so the same rule fires again even
        // though the statistics still describe the original session.
        sendStats(stats(IP, 1_000, 11));
        assertThat(matches()).hasSize(2);
    }

    @Test
    void keepsStateWhileTheSessionIsStillActive() throws Exception {
        sendRule(new Rule("r1", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, true));
        sendStats(stats(IP, 1_000, 10));

        IpStats later = stats(IP, 1_000, 11);
        later.setLastSeenMs(SessionExpiry.GAP_MS);
        sendStats(later);

        advanceBothInputsTo(1_000 + SessionExpiry.GAP_MS + 1);

        assertThat(harness.numEventTimeTimers())
                .as("the session is still active, so the timer must have been re-registered")
                .isPositive();
    }

    @Test
    void firesOnTheErrorRatioRegardlessOfVolume() throws Exception {
        sendRule(new Rule("failing", Metric.ERROR_RATIO_AT_LEAST, 0.5, 10, true));

        // 80% of 50 requests failed: well over the threshold.
        sendStats(statsWithErrors(50, 40));

        assertThat(matches()).singleElement()
                .extracting(RuleMatch::getObservedValue)
                .isEqualTo(0.8);
    }

    @Test
    void doesNotFireOnARatioBelowTheThreshold() throws Exception {
        sendRule(new Rule("failing", Metric.ERROR_RATIO_AT_LEAST, 0.5, 10, true));

        // A steady 5% failure rate: this stays below the threshold no matter how long the session
        // runs, which is the point of using a ratio rather than a count.
        sendStats(statsWithErrors(100, 5));
        sendStats(statsWithErrors(10_000, 500));

        assertThat(matches()).isEmpty();
    }

    @Test
    void ignoresARatioUntilTheSessionHasEnoughRequests() throws Exception {
        sendRule(new Rule("failing", Metric.ERROR_RATIO_AT_LEAST, 0.5, 20, true));

        // A single failed request is a 100% error ratio, and means nothing.
        sendStats(statsWithErrors(1, 1));
        assertThat(matches()).isEmpty();

        sendStats(statsWithErrors(19, 19));
        assertThat(matches()).as("still one request short of the minimum").isEmpty();

        sendStats(statsWithErrors(20, 19));
        assertThat(matches()).hasSize(1);
    }

    @Test
    void firesWhenDistinctPathsAreAtMostTheThreshold() throws Exception {
        sendRule(new Rule("one-path", Metric.DISTINCT_PATHS_AT_MOST, 2, 20, true));

        sendStats(statsWithPaths(50, 1));

        assertThat(matches()).singleElement()
                .extracting(RuleMatch::getObservedValue)
                .isEqualTo(1.0);
    }

    @Test
    void doesNotFireWhenDistinctPathsAreAboveTheThreshold() throws Exception {
        sendRule(new Rule("one-path", Metric.DISTINCT_PATHS_AT_MOST, 2, 20, true));

        sendStats(statsWithPaths(50, 5));

        assertThat(matches()).isEmpty();
    }

    @Test
    void ignoresDistinctPathsUntilTheSessionHasEnoughRequests() throws Exception {
        sendRule(new Rule("one-path", Metric.DISTINCT_PATHS_AT_MOST, 2, 20, true));

        // Without the minimum, the very first request of every session would match: one request
        // means one distinct path, which is at most two.
        sendStats(statsWithPaths(1, 1));

        assertThat(matches()).isEmpty();
    }

    @Test
    void thresholdIsInclusiveInBothDirections() throws Exception {
        sendRule(new Rule("at-least", Metric.TOTAL_REQUESTS_AT_LEAST, 10, 0, true));
        sendRule(new Rule("at-most", Metric.DISTINCT_PATHS_AT_MOST, 3, 0, true));

        sendStats(statsWithPaths(10, 3));

        assertThat(matches()).extracting(RuleMatch::getRuleId)
                .containsExactlyInAnyOrder("at-least", "at-most");
    }

    private void advanceBothInputsTo(long timestampMs) throws Exception {
        harness.processWatermark(new Watermark(timestampMs));
        harness.processBroadcastWatermark(new Watermark(timestampMs));
    }
}
