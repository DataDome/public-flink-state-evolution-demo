package co.datadome.demo.flink.operator;

import co.datadome.demo.flink.model.HttpRequest;
import co.datadome.demo.flink.model.Stats;
import co.datadome.demo.flink.state.StatsSerializer;
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
 * <p>Statistics accumulate for as long as requests keep arriving for the same IP address, and are discarded after
 * {@link SessionExpiration#GAP} without activity.
 */
public final class IpStatsFunction extends KeyedProcessFunction<String, HttpRequest, Stats> {

    private static final long serialVersionUID = 1L;

    /**
     * Declared with an explicit {@link StatsSerializer}, so it's not using the
     * {@link org.apache.flink.api.java.typeutils.runtime.PojoSerializer}.
     */
    private static final ValueStateDescriptor<Stats> IP_STATS_DESCRIPTOR =
            new ValueStateDescriptor<>("ipStats", new StatsSerializer());

    private static final MapStateDescriptor<String, Boolean> SEEN_PATHS_DESCRIPTOR =
            new MapStateDescriptor<>("seenPaths", String.class, Boolean.class);

    private transient ValueState<Stats> statsState;

    /**
     * Paths already seen in this session, backing {@link Stats#getDistinctPathCount()}.
     */
    private transient MapState<String, Boolean> seenPathsState;

    @Override
    public void open(OpenContext openContext) {
        statsState = getRuntimeContext().getState(IP_STATS_DESCRIPTOR);
        seenPathsState = getRuntimeContext().getMapState(SEEN_PATHS_DESCRIPTOR);
    }

    @Override
    public void processElement(HttpRequest request, Context ctx, Collector<Stats> out) throws Exception {
        Stats stats = statsState.value();
        if (stats == null) {
            stats = Stats.startingWith(request);
            // Only registered once per session; onTimer re-registers it for as long as the session
            // stays alive, which keeps this to one timer per key instead of one per request.
            ctx.timerService().registerEventTimeTimer(request.getTimestampMs() + SessionExpiration.GAP_MS);
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
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Stats> out) throws Exception {
        Stats stats = statsState.value();
        if (stats == null) {
            return;
        }

        long expiresAt = stats.getLastSeenMs() + SessionExpiration.GAP_MS;
        if (timestamp < expiresAt) {
            // Requests arrived since this timer was set, so the session is still alive.
            ctx.timerService().registerEventTimeTimer(expiresAt);
            return;
        }

        statsState.clear();
        seenPathsState.clear();
    }
}
