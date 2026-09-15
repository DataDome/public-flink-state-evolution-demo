package co.datadome.demo.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The result of a rule firing for an IP address, as written to the output Kafka topic.
 *
 * <p>This class is mutable, and has a public no-argument constructor, because Flink only recognises
 * a type as a POJO (and therefore only uses {@code PojoSerializer} for it) under those conditions.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public final class RuleMatch {

    private String ruleId;
    private String ip;
    private Metric metric;

    /** Value the metric had reached when the rule fired. */
    private double observedValue;

    private double threshold;

    /** Event time at which the rule fired. */
    private long detectedAtMs;

    /** Creates the result of that rule firing on those statistics. */
    public static RuleMatch of(Rule rule, IpStats stats) {
        return new RuleMatch(
                rule.getRuleId(),
                stats.getIp(),
                rule.getMetric(),
                rule.getMetric().extract(stats),
                rule.getThreshold(),
                stats.getLastSeenMs()
        );
    }
}
