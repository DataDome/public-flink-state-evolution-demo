package co.datadome.demo.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * A detection rule, as read from the rules Kafka topic and held in broadcast state.
 *
 * <p>A rule fires when the {@link Metric} it selects reaches its threshold for a given IP address.
 * Publishing a rule with the same id replaces the previous version; publishing it with
 * {@code isEnabled} set to false removes it.
 *
 * <p>This class is mutable, and has a public no-argument constructor, because Flink only recognises
 * a type as a POJO (and therefore only uses {@code PojoSerializer} for it) under those conditions.
 */
public final class Rule {

    private String ruleId;
    private Metric metric;
    private long threshold;

    /**
     * Explicitly named for Jackson. Flink requires the setter to be {@code setIsEnabled} for a
     * field called {@code isEnabled}, but Jackson derives "enabled" from the {@code isEnabled()}
     * getter and "isEnabled" from that setter, and would treat them as two different properties.
     */
    @JsonProperty("isEnabled")
    private boolean isEnabled;

    /** Required by Flink's POJO serializer. */
    public Rule() {}

    public Rule(String ruleId, Metric metric, long threshold, boolean isEnabled) {
        this.ruleId = ruleId;
        this.metric = metric;
        this.threshold = threshold;
        this.isEnabled = isEnabled;
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

    public long getThreshold() {
        return threshold;
    }

    public void setThreshold(long threshold) {
        this.threshold = threshold;
    }

    /**
     * Named to match Flink's POJO field detection: for the field {@code isEnabled}, Flink accepts
     * {@code isEnabled()} as the getter but requires {@code setIsEnabled} as the setter.
     */
    @JsonProperty("isEnabled")
    public boolean isEnabled() {
        return isEnabled;
    }

    public void setIsEnabled(boolean isEnabled) {
        this.isEnabled = isEnabled;
    }

    /** Whether that statistics record reaches this rule's threshold. */
    public boolean matches(IpStats stats) {
        return isEnabled && metric.extract(stats) >= threshold;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Rule that)) {
            return false;
        }
        return threshold == that.threshold
                && isEnabled == that.isEnabled
                && Objects.equals(ruleId, that.ruleId)
                && metric == that.metric;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleId, metric, threshold, isEnabled);
    }

    @Override
    public String toString() {
        return "Rule{ruleId='%s', metric=%s, threshold=%d, isEnabled=%b}"
                .formatted(ruleId, metric, threshold, isEnabled);
    }
}
