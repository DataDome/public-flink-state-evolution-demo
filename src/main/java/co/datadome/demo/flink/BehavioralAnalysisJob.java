package co.datadome.demo.flink;

import co.datadome.demo.flink.model.HttpRequest;
import co.datadome.demo.flink.model.Rule;
import co.datadome.demo.flink.model.RuleMatch;
import co.datadome.demo.flink.model.Stats;
import co.datadome.demo.flink.operator.IpStatsFunction;
import co.datadome.demo.flink.operator.RuleEvaluationFunction;
import co.datadome.demo.flink.serde.JsonDeserializer;
import co.datadome.demo.flink.serde.JsonSerializer;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

/**
 * A deliberately simplified behavioral analysis pipeline, used to demonstrate how Flink state can
 * be evolved across versions of a job.
 *
 * <p>The pipeline reads HTTP requests and detection rules from Kafka, accumulates per-IP statistics
 * over a session, evaluates the rules against those statistics, and writes the matches back to
 * Kafka:
 *
 * <pre>
 *   requests --&gt; [ IpStatsFunction ] --&gt; stats --&gt; [ RuleEvaluationFunction ] --&gt; matches
 *   rules ----------------------- broadcast --------&gt;        ^
 * </pre>
 *
 * <p>See {@code docs/state-evolution.md} for what this job does to stay restorable.
 */
public final class BehavioralAnalysisJob {

    /**
     * Lateness tolerated on the requests stream before a record is considered late.
     */
    private static final Duration MAX_OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    /**
     * How long the rules stream may stay silent before it stops holding back the watermark. Rules
     * arrive rarely, and a two-input operator advances its watermark at the pace of its slowest
     * input, so without this the session expiry timers would never fire.
     */
    private static final Duration RULES_IDLENESS = Duration.ofSeconds(1);

    /**
     * How long a requests partition may stay silent before it stops holding back the watermark.
     * Without this, a single quiet Kafka partition would stall event time for the whole job.
     */
    private static final Duration REQUESTS_IDLENESS = Duration.ofSeconds(10);

    private static final Duration CHECKPOINT_INTERVAL = Duration.ofSeconds(10);

    private BehavioralAnalysisJob() {
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String brokers = params.get("bootstrap-servers", "localhost:9092");
        String requestsTopic = params.get("requests-topic", "http-requests");
        String rulesTopic = params.get("rules-topic", "rules");
        String matchesTopic = params.get("matches-topic", "rule-matches");
        String groupId = params.get("group-id", "behavioral-analysis");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config());
        buildPipeline(env, brokers, requestsTopic, rulesTopic, matchesTopic, groupId);
        env.execute("Behavioral analysis");
    }

    /**
     * Configuration this job requires in order to be restorable. The state backend, the checkpoint
     * directory and the savepoint directory are deliberately left to the cluster configuration, so
     * that the demo can switch between backends without rebuilding the jar.
     */
    static Configuration config() {
        Configuration config = new Configuration();

        // Any type Flink cannot serialize as a POJO would silently fall back to Kryo, which does
        // not support schema evolution. Failing at submission is much better than discovering it
        // when a savepoint cannot be restored.
        config.set(PipelineOptions.GENERIC_TYPES, false);

        // Generated operator ids change whenever the job graph changes, which breaks restore. This
        // forces every operator below to carry an explicit uid.
        config.set(PipelineOptions.AUTO_GENERATE_UIDS, false);

        config.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, CHECKPOINT_INTERVAL);
        return config;
    }

    /**
     * Builds the pipeline on that environment. Extracted from {@link #main} so tests can reuse it.
     */
    static void buildPipeline(
            StreamExecutionEnvironment env,
            String brokers,
            String requestsTopic,
            String rulesTopic,
            String matchesTopic,
            String groupId) {

        KafkaSource<HttpRequest> requestsSource =
                KafkaSource.<HttpRequest>builder()
                        .setBootstrapServers(brokers)
                        .setTopics(requestsTopic)
                        .setGroupId(groupId + "-requests")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new JsonDeserializer<>(HttpRequest.class))
                        .build();

        KafkaSource<Rule> rulesSource =
                KafkaSource.<Rule>builder()
                        .setBootstrapServers(brokers)
                        .setTopics(rulesTopic)
                        .setGroupId(groupId + "-rules")
                        // Rules are a changelog of the full rule set, so always replay them all.
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new JsonDeserializer<>(Rule.class))
                        .build();

        KafkaSink<RuleMatch> matchesSink =
                KafkaSink.<RuleMatch>builder()
                        .setBootstrapServers(brokers)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.<RuleMatch>builder()
                                        .setTopic(matchesTopic)
                                        .setValueSerializationSchema(new JsonSerializer<RuleMatch>())
                                        .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

        WatermarkStrategy<HttpRequest> requestsWatermarks =
                WatermarkStrategy.<HttpRequest>forBoundedOutOfOrderness(MAX_OUT_OF_ORDERNESS)
                        .withTimestampAssigner((request, recordTimestamp) -> request.getTimestampMs())
                        .withIdleness(REQUESTS_IDLENESS);

        WatermarkStrategy<Rule> rulesWatermarks = WatermarkStrategy.<Rule>noWatermarks().withIdleness(RULES_IDLENESS);

        DataStream<Stats> stats =
                env.fromSource(requestsSource, requestsWatermarks, "HTTP requests")
                        .uid("source-http-requests")
                        .keyBy(HttpRequest::getIp)
                        .process(new IpStatsFunction())
                        .uid("ip-stats")
                        .name("IP statistics");

        BroadcastStream<Rule> rules =
                env.fromSource(rulesSource, rulesWatermarks, "Rules")
                        .uid("source-rules")
                        .broadcast(RuleEvaluationFunction.RULES_DESCRIPTOR);

        DataStream<RuleMatch> matches =
                stats.keyBy(Stats::getIp)
                        .connect(rules)
                        .process(new RuleEvaluationFunction())
                        .uid("rule-evaluation")
                        .name("Rule evaluation");

        matches.sinkTo(matchesSink).uid("sink-rule-matches").name("Rule matches");
    }
}
