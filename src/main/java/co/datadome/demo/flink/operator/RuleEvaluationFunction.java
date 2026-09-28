package co.datadome.demo.flink.operator;

import co.datadome.demo.flink.model.IpStats;
import co.datadome.demo.flink.model.Rule;
import co.datadome.demo.flink.model.RuleMatch;
import co.datadome.demo.flink.state.RuleSerializer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Evaluates the broadcast rules against the statistics of each IP address.
 *
 * <p>Rules arrive on the broadcast side and are held in broadcast state, keyed by rule id. The
 * statistics arrive on the keyed side. A rule fires at most once per session per IP address.
 */
public final class RuleEvaluationFunction extends KeyedBroadcastProcessFunction<String, IpStats, Rule, RuleMatch> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RuleEvaluationFunction.class);

    /**
     * Declared with an explicit {@link RuleSerializer}, so it's not using the
     * {@link org.apache.flink.api.java.typeutils.runtime.PojoSerializer}.
     */
    public static final MapStateDescriptor<String, Rule> RULES_DESCRIPTOR =
            new MapStateDescriptor<>("rules", StringSerializer.INSTANCE, new RuleSerializer());

    private static final MapStateDescriptor<String, Boolean> FIRED_RULE_IDS_DESCRIPTOR = new MapStateDescriptor<>("firedRuleIds", String.class, Boolean.class);

    /**
     * Ids of the rules that already fired for the current session of this key.
     */
    private transient MapState<String, Boolean> firedRuleIdsState;

    /**
     * Session this operator last saw for this key, used to detect that the session rolled over and
     * that {@link #firedRuleIdsState} must be cleared.
     */
    private transient ValueState<Long> sessionStartState;

    /**
     * Last activity seen for this key, used only to expire the state above.
     */
    private transient ValueState<Long> lastSeenState;

    @Override
    public void open(OpenContext openContext) {
        firedRuleIdsState = getRuntimeContext().getMapState(FIRED_RULE_IDS_DESCRIPTOR);
        sessionStartState = getRuntimeContext().getState(new ValueStateDescriptor<>("sessionStart", Long.class));
        lastSeenState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastSeen", Long.class));
    }

    @Override
    public void processElement(IpStats stats, ReadOnlyContext ctx, Collector<RuleMatch> out) throws Exception {
        Long knownSessionStart = sessionStartState.value();
        if (knownSessionStart == null) {
            // Only registered once per session; onTimer re-registers it while the session is alive.
            ctx.timerService().registerEventTimeTimer(stats.getLastSeenMs() + SessionExpiration.GAP_MS);
        }
        if (knownSessionStart == null || stats.getSessionStartMs() > knownSessionStart) {
            // The upstream session expired and a new one started, so rules may fire again.
            firedRuleIdsState.clear();
            sessionStartState.update(stats.getSessionStartMs());
        }
        lastSeenState.update(stats.getLastSeenMs());

        for (Map.Entry<String, Rule> entry : ctx.getBroadcastState(RULES_DESCRIPTOR).immutableEntries()) {
            Rule rule = entry.getValue();
            if (firedRuleIdsState.contains(rule.getRuleId())) {
                continue;
            }
            if (rule.matches(stats)) {
                firedRuleIdsState.put(rule.getRuleId(), Boolean.TRUE);
                out.collect(RuleMatch.of(rule, stats));
            }
        }
    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx, Collector<RuleMatch> out) throws Exception {
        BroadcastState<String, Rule> rules = ctx.getBroadcastState(RULES_DESCRIPTOR);
        if (rule.isEnabled()) {
            LOG.info("Applying rule {}", rule);
            rules.put(rule.getRuleId(), rule);
        } else {
            LOG.info("Removing rule {}", rule.getRuleId());
            rules.remove(rule.getRuleId());
        }

        // A rule that was just published is allowed to fire again, even for sessions where an
        // earlier version of it already fired.
        String ruleId = rule.getRuleId();
        ctx.applyToKeyedState(FIRED_RULE_IDS_DESCRIPTOR, (key, state) -> state.remove(ruleId));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RuleMatch> out) throws Exception {
        Long lastSeen = lastSeenState.value();
        if (lastSeen == null) {
            return;
        }

        long expiresAt = lastSeen + SessionExpiration.GAP_MS;
        if (timestamp < expiresAt) {
            ctx.timerService().registerEventTimeTimer(expiresAt);
            return;
        }

        firedRuleIdsState.clear();
        sessionStartState.clear();
        lastSeenState.clear();
    }
}
