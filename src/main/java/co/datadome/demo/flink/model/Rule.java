package co.datadome.demo.flink.model;

import java.util.Objects;

/**
 * A detection rule, as read from the rules Kafka topic and held in broadcast state.
 *
 * <p>A rule fires when the {@link Metric} it selects reaches its threshold for a given IP address,
 * once that session has at least {@link #getMinTotalRequests()} requests. Publishing a rule with
 * the same id replaces the previous version; publishing it with {@code enabled} set to false
 * removes it.
 *
 * <p>This class is mutable, and has a public no-argument constructor, because Flink only recognises
 * a type as a POJO (and therefore only uses {@code PojoSerializer} for it) under those conditions.
 */
public final class Rule {

    private String ruleId;
    private Metric metric;

    /**
     * Compared against the metric in the direction the metric's name states. A double because
     * {@link Metric#ERROR_RATIO_AT_LEAST} is a ratio between 0 and 1, while the other metrics are
     * counts.
     */
    private double threshold;

    /**
     * Number of requests a session must have before this rule is evaluated at all.
     *
     * <p>For example, The first failed request of a session is a 100% error ratio, and
     * {@link Metric#DISTINCT_PATHS_AT_MOST} would match every session on its first request.
     */
    private long minTotalRequests;

    private boolean enabled;

    /** Required by Flink's POJO serializer. */
    public Rule() {}

    public Rule(
            String ruleId,
            Metric metric,
            double threshold,
            long minTotalRequests,
            boolean enabled) {
        this.ruleId = ruleId;
        this.metric = metric;
        this.threshold = threshold;
        this.minTotalRequests = minTotalRequests;
        this.enabled = enabled;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public Metric getMetric() {
        return metric;
    }

    public void setMetric(Metric metric) {
        this.metric = metric;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public long getMinTotalRequests() {
        return minTotalRequests;
    }

    public void setMinTotalRequests(long minTotalRequests) {
        this.minTotalRequests = minTotalRequests;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /** Whether that statistics record reaches this rule's threshold. */
    public boolean matches(IpStats stats) {
        if (!enabled || stats.getTotalCount() < minTotalRequests) {
            return false;
        }
        return metric.isReached(stats, threshold);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Rule that)) {
            return false;
        }
        return Double.compare(threshold, that.threshold) == 0
                && minTotalRequests == that.minTotalRequests
                && enabled == that.enabled
                && Objects.equals(ruleId, that.ruleId)
                && metric == that.metric;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleId, metric, threshold, minTotalRequests, enabled);
    }

    @Override
    public String toString() {
        return "Rule{ruleId='%s', metric=%s, threshold=%s, minTotalRequests=%d, enabled=%b}"
                .formatted(ruleId, metric, threshold, minTotalRequests, enabled);
    }
}
