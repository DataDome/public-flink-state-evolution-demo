package co.datadome.demo.flink.model;

import static org.assertj.core.api.Assertions.assertThat;

import co.datadome.demo.flink.serde.JsonDeserializer;
import co.datadome.demo.flink.serde.JsonSerializer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Checks that what the job writes to Kafka is what it can read back, and that the JSON published
 * by hand during the demo (see {@code scripts/publish-rule.sh}) is understood.
 */
class JsonRoundTripTest {

    private static <T> byte[] write(T value) throws Exception {
        JsonSerializer<T> serializer = new JsonSerializer<>();
        serializer.open(null);
        return serializer.serialize(value);
    }

    private static <T> T read(Class<T> type, String json) throws Exception {
        JsonDeserializer<T> deserializer = new JsonDeserializer<>(type);
        deserializer.open(null);
        return deserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void ruleSurvivesARoundTrip() throws Exception {
        Rule rule = new Rule("r1", Metric.ERROR_REQUESTS, 20, true);
        assertThat(read(Rule.class, new String(write(rule), StandardCharsets.UTF_8)))
                .isEqualTo(rule);
    }

    @Test
    void ruleIsReadFromTheJsonThePublishScriptSends() throws Exception {
        String json =
                "{\"ruleId\":\"too-many-errors\",\"metric\":\"ERROR_REQUESTS\","
                        + "\"threshold\":20,\"isEnabled\":true}";
        assertThat(read(Rule.class, json))
                .isEqualTo(new Rule("too-many-errors", Metric.ERROR_REQUESTS, 20, true));
    }

    @Test
    void aDisabledRuleIsReadAsDisabled() throws Exception {
        String json =
                "{\"ruleId\":\"r1\",\"metric\":\"ERROR_REQUESTS\","
                        + "\"threshold\":20,\"isEnabled\":false}";
        assertThat(read(Rule.class, json).isEnabled()).isFalse();
    }

    @Test
    void httpRequestSurvivesARoundTrip() throws Exception {
        HttpRequest request = new HttpRequest(1_000, "10.0.0.1", "/login", "curl/8", 403);
        assertThat(read(HttpRequest.class, new String(write(request), StandardCharsets.UTF_8)))
                .isEqualTo(request);
    }

    @Test
    void ruleMatchSurvivesARoundTrip() throws Exception {
        RuleMatch match =
                RuleMatch.of(
                        new Rule("r1", Metric.TOTAL_REQUESTS, 10, true),
                        IpStats.startingWith(new HttpRequest(1_000, "10.0.0.1", "/a", "curl/8", 200)));
        assertThat(read(RuleMatch.class, new String(write(match), StandardCharsets.UTF_8)))
                .isEqualTo(match);
    }

    @Test
    void malformedJsonIsSkippedRatherThanFailingTheJob() throws Exception {
        assertThat(read(Rule.class, "{not json")).isNull();
    }
}
