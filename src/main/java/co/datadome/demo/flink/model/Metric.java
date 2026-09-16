package co.datadome.demo.flink.model;

/**
 * The statistic a {@link Rule} compares against its threshold, together with the direction of the
 * comparison.
 *
 * <p>This enum is serialized by Flink's {@code EnumSerializer}. Adding a new constant is a compatible change; renaming
 * or removing one is not.
 */
public enum Metric {

    /**
     * Total number of requests in the session. Fires when it is greater than or equal to the threshold.
     */
    TOTAL_REQUESTS_AT_LEAST,

    /**
     * Share of the session's requests that failed, between 0 and 1. Fires when it is greater than
     * or equal to the threshold.
     */
    ERROR_RATIO_AT_LEAST,

    /**
     * Number of distinct paths visited in the session. Fires when it is less than or equal to the threshold (low values
     * are the suspicious ones: many requests to a single path is not browsing).
     */
    DISTINCT_PATHS_AT_MOST;

    /**
     * Reads the value of this metric out of that statistics record.
     */
    public double extract(Stats stats) {
        return switch (this) {
            case TOTAL_REQUESTS_AT_LEAST -> stats.getTotalCount();
            case ERROR_RATIO_AT_LEAST -> stats.getErrorRatio();
            case DISTINCT_PATHS_AT_MOST -> stats.getDistinctPathCount();
        };
    }

    /**
     * Whether this metric fires on those stats for that threshold.
     */
    public boolean isReached(Stats stats, double threshold) {
        return switch (this) {
            case TOTAL_REQUESTS_AT_LEAST -> stats.getTotalCount() >= threshold;
            case ERROR_RATIO_AT_LEAST -> stats.getErrorRatio() >= threshold;
            case DISTINCT_PATHS_AT_MOST -> stats.getDistinctPathCount() <= threshold;
        };
    }
}

