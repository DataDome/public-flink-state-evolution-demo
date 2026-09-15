package co.datadome.demo.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A heavily simplified HTTP request, as read from the input Kafka topic.
 *
 * <p>This class is mutable, and has a public no-argument constructor, because Flink only recognises
 * a type as a POJO (and therefore only uses {@code PojoSerializer} for it) under those conditions.
 * Instances should be treated as immutable once handed to Flink.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public final class HttpRequest {

    /** Event time of that request. */
    private long timestampMs;

    private String ip;
    private String path;
    private String userAgent;
    private int statusCode;

    /**
     * Whether that request failed, which is what {@link Metric#ERROR_RATIO_AT_LEAST} counts.
     *
     * <p>Derived from the status code, so it is kept out of the JSON written to Kafka: Jackson
     * would otherwise publish it as an "error" field that nothing ever reads back.
     */
    @JsonIgnore
    public boolean isError() {
        return statusCode >= 400;
    }
}
