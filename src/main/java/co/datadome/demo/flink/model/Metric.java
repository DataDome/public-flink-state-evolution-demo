package co.datadome.demo.flink.model;

/**
 * The statistic a {@link Rule} compares against its threshold.
 *
 * <p>Rules are evaluated against statistics that were already aggregated, so a rule cannot filter
 * on individual requests: it can only select one of the dimensions {@link IpStats} precomputes.
 *
 * <p>This enum is part of the broadcast state, so it is serialized by Flink's {@code EnumSerializer}.
 * Adding a new constant is a compatible change; renaming or removing one is not.
 */
public enum Metric {
    TOTAL_REQUESTS,
    ERROR_REQUESTS,
    DISTINCT_PATHS;

    /** Reads the value of this metric out of that statistics record. */
    public long extract(IpStats stats) {
        return switch (this) {
            case TOTAL_REQUESTS -> stats.getTotalCount();
            case ERROR_REQUESTS -> stats.getErrorCount();
            case DISTINCT_PATHS -> stats.getDistinctPathCount();
        };
    }
}
