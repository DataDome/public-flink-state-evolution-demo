package co.datadome.demo.flink.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
@Data
@NoArgsConstructor
@AllArgsConstructor
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
     * <p>For example, the first failed request of a session is a 100% error ratio, and
     * {@link Metric#DISTINCT_PATHS_AT_MOST} would match every session on its first request.
     */
    private long minTotalRequests;

    private boolean enabled;

    /** Whether that statistics record reaches this rule's threshold. */
    public boolean matches(Stats stats) {
        if (!enabled || stats.getTotalCount() < minTotalRequests) {
            return false;
        }
        return metric.isReached(stats, threshold);
    }
}
