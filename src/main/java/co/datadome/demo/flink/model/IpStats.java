package co.datadome.demo.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

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
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
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
     * Share of this session's requests that failed, between 0 and 1.
     *
     * <p>Derived rather than stored, because only the counts can be accumulated. Annotated to keep
     * it out of any JSON rendering of this record.
     */
    @JsonIgnore
    public double getErrorRatio() {
        return totalCount == 0 ? 0.0 : (double) errorCount / totalCount;
    }

    /**
     * Returns an independent copy of this record.
     *
     * <p>The operator keeps mutating the instance it holds in state, so what it emits downstream
     * has to be a snapshot: otherwise every record already emitted would keep changing.
     *
     * <p>Goes through the builder so that it stays correct if a field is added or reordered.
     */
    public IpStats copy() {
        return toBuilder().build();
    }
}
