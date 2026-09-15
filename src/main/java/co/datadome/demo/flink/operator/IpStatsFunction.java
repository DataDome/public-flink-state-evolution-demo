package co.datadome.demo.flink.operator;

import co.datadome.demo.flink.model.HttpRequest;
import co.datadome.demo.flink.model.IpStats;
import co.datadome.demo.flink.state.IpStatsSerializer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Accumulates per-IP statistics over a session, and emits the updated statistics on every request.
 *
 * <p>There is no window. Statistics accumulate for as long as requests keep arriving for the same
 * IP address, and are discarded after {@link SessionExpiry#GAP} without activity. Emitting on every
 * request means a rule fires as soon as its threshold is crossed, rather than at a window boundary.
 */
public final class IpStatsFunction extends KeyedProcessFunction<String, HttpRequest, IpStats> {

    private static final long serialVersionUID = 1L;

    /**
     * Declared with an explicit {@link IpStatsSerializer} rather than from the type, so that the
     * demo controls the state layout itself.
     */
    private static final ValueStateDescriptor<IpStats> IP_STATS_DESCRIPTOR =
            new ValueStateDescriptor<>("ipStats", new IpStatsSerializer());

    private static final MapStateDescriptor<String, Boolean> SEEN_PATHS_DESCRIPTOR =
            new MapStateDescriptor<>("seenPaths", String.class, Boolean.class);

    /** The accumulated statistics. This is the piece of state the evolution demo revolves around. */
    private transient ValueState<IpStats> statsState;

    /**
     * Paths already seen in this session, backing {@link IpStats#getDistinctPathCount()}.
     *
     * <p>Deliberately a {@code MapState} and not a {@code Set} inside {@link IpStats}: a collection
     * field in a POJO falls back to Kryo, which cannot evolve, and the job disables generic types
     * so that such a fallback fails loudly instead of silently.
     */
    private transient MapState<String, Boolean> seenPathsState;

    @Override
    public void open(OpenContext openContext) {
        statsState = getRuntimeContext().getState(IP_STATS_DESCRIPTOR);
        seenPathsState = getRuntimeContext().getMapState(SEEN_PATHS_DESCRIPTOR);
    }

    @Override
    public void processElement(HttpRequest request, Context ctx, Collector<IpStats> out) throws Exception {
        IpStats stats = statsState.value();
        if (stats == null) {
            stats = IpStats.startingWith(request);
            // Only registered once per session; onTimer re-registers it for as long as the session
            // stays alive, which keeps this to one timer per key instead of one per request.
            ctx.timerService().registerEventTimeTimer(request.getTimestampMs() + SessionExpiry.GAP_MS);
        }

        stats.accumulate(request);
        if (!seenPathsState.contains(request.getPath())) {
            seenPathsState.put(request.getPath(), Boolean.TRUE);
            stats.setDistinctPathCount(stats.getDistinctPathCount() + 1);
        }
        statsState.update(stats);

        out.collect(stats.copy());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<IpStats> out) throws Exception {
        IpStats stats = statsState.value();
        if (stats == null) {
            return;
        }

        long expiresAt = stats.getLastSeenMs() + SessionExpiry.GAP_MS;
        if (timestamp < expiresAt) {
            // Requests arrived since this timer was set, so the session is still alive.
            ctx.timerService().registerEventTimeTimer(expiresAt);
            return;
        }

        statsState.clear();
        seenPathsState.clear();
    }
}
