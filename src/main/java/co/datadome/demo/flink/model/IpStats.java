package co.datadome.demo.flink.model;

import java.util.Objects;

/**
 * Statistics accumulated for a single IP address over the course of a session.
 *
 * <p>A session lasts for as long as requests keep arriving for that IP address, and is discarded
 * after a long period of inactivity. There is deliberately no window: this record can live for a
 * very long time, which is exactly what makes its schema interesting to evolve.
 *
 * <p>This class is the main subject of the state evolution demo. It is held in {@code ValueState},
 * so any change made to the fields below changes the layout of every savepoint taken so far. See
 * {@code docs/state-evolution.md}.
 *
 * <p>This class is mutable, and has a public no-argument constructor, because Flink only recognises
 * a type as a POJO (and therefore only uses {@code PojoSerializer} for it) under those conditions.
 * It is also updated in place on every request, which avoids allocating a new record per event.
 */
public final class IpStats {

    private String ip;

    /** Event time of the first request of this session. */
    private long sessionStartMs;

    /** Event time of the most recent request of this session, used to detect inactivity. */
    private long lastSeenMs;

    private long totalCount;
    private long errorCount;

    /**
     * Number of distinct paths seen in this session. The paths themselves are not kept here: they
     * live in a separate {@code MapState} in the operator, and this field is the running count.
     */
    private int distinctPathCount;

    /** Required by Flink's POJO serializer. */
    public IpStats() {}

    /** Creates the statistics for a session starting with that request. */
    public static IpStats startingWith(HttpRequest request) {
        IpStats stats = new IpStats();
        stats.ip = request.getIp();
        stats.sessionStartMs = request.getTimestampMs();
        stats.lastSeenMs = request.getTimestampMs();
        return stats;
    }

    /**
     * Accumulates that request into this record.
     *
     * <p>The distinct path count is not handled here, because deciding whether a path is new
     * requires state the operator owns. Use {@link #setDistinctPathCount(int)} for that.
     */
    public void accumulate(HttpRequest request) {
        totalCount++;
        if (request.isError()) {
            errorCount++;
        }
        // Requests can be slightly out of order within the allowed lateness, so never move backwards.
        lastSeenMs = Math.max(lastSeenMs, request.getTimestampMs());
    }

    /**
     * Returns an independent copy of this record.
     *
     * <p>The operator keeps mutating the instance it holds in state, so what it emits downstream
     * has to be a snapshot: otherwise every record already emitted would keep changing.
     */
    public IpStats copy() {
        IpStats copy = new IpStats();
        copy.ip = ip;
        copy.sessionStartMs = sessionStartMs;
        copy.lastSeenMs = lastSeenMs;
        copy.totalCount = totalCount;
        copy.errorCount = errorCount;
        copy.distinctPathCount = distinctPathCount;
        return copy;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public long getSessionStartMs() {
        return sessionStartMs;
    }

    public void setSessionStartMs(long sessionStartMs) {
        this.sessionStartMs = sessionStartMs;
    }

    public long getLastSeenMs() {
        return lastSeenMs;
    }

    public void setLastSeenMs(long lastSeenMs) {
        this.lastSeenMs = lastSeenMs;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(long errorCount) {
        this.errorCount = errorCount;
    }

    public int getDistinctPathCount() {
        return distinctPathCount;
    }

    public void setDistinctPathCount(int distinctPathCount) {
        this.distinctPathCount = distinctPathCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IpStats that)) {
            return false;
        }
        return sessionStartMs == that.sessionStartMs
                && lastSeenMs == that.lastSeenMs
                && totalCount == that.totalCount
                && errorCount == that.errorCount
                && distinctPathCount == that.distinctPathCount
                && Objects.equals(ip, that.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, sessionStartMs, lastSeenMs, totalCount, errorCount, distinctPathCount);
    }

    @Override
    public String toString() {
        return ("IpStats{ip='%s', sessionStartMs=%d, lastSeenMs=%d, totalCount=%d, errorCount=%d, "
                        + "distinctPathCount=%d}")
                .formatted(ip, sessionStartMs, lastSeenMs, totalCount, errorCount, distinctPathCount);
    }
}
