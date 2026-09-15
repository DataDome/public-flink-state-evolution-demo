package co.datadome.demo.flink.model;

import java.util.Objects;

/**
 * The result of a rule firing for an IP address, as written to the output Kafka topic.
 *
 * <p>This class is mutable, and has a public no-argument constructor, because Flink only recognises
 * a type as a POJO (and therefore only uses {@code PojoSerializer} for it) under those conditions.
 */
public final class RuleMatch {

    private String ruleId;
    private String ip;
    private Metric metric;

    /** Value the metric had reached when the rule fired. */
    private double observedValue;

    private double threshold;

    /** Event time at which the rule fired. */
    private long detectedAtMs;

    /** Required by Flink's POJO serializer. */
    public RuleMatch() {}

    /** Creates the result of that rule firing on those statistics. */
    public static RuleMatch of(Rule rule, IpStats stats) {
        RuleMatch match = new RuleMatch();
        match.ruleId = rule.getRuleId();
        match.ip = stats.getIp();
        match.metric = rule.getMetric();
        match.observedValue = rule.getMetric().extract(stats);
        match.threshold = rule.getThreshold();
        match.detectedAtMs = stats.getLastSeenMs();
        return match;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Metric getMetric() {
        return metric;
    }

    public void setMetric(Metric metric) {
        this.metric = metric;
    }

    public double getObservedValue() {
        return observedValue;
    }

    public void setObservedValue(double observedValue) {
        this.observedValue = observedValue;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public long getDetectedAtMs() {
        return detectedAtMs;
    }

    public void setDetectedAtMs(long detectedAtMs) {
        this.detectedAtMs = detectedAtMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RuleMatch that)) {
            return false;
        }
        return Double.compare(observedValue, that.observedValue) == 0
                && Double.compare(threshold, that.threshold) == 0
                && detectedAtMs == that.detectedAtMs
                && Objects.equals(ruleId, that.ruleId)
                && Objects.equals(ip, that.ip)
                && metric == that.metric;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleId, ip, metric, observedValue, threshold, detectedAtMs);
    }

    @Override
    public String toString() {
        return ("RuleMatch{ruleId='%s', ip='%s', metric=%s, observedValue=%s, threshold=%s, "
                        + "detectedAtMs=%d}")
                .formatted(ruleId, ip, metric, observedValue, threshold, detectedAtMs);
    }
}
