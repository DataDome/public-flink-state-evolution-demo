package co.datadome.demo.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A heavily simplified HTTP request, as read from the input Kafka topic.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public final class HttpRequest {

    /**
     * Event time of that request.
     */
    private long timestampMs;

    private String ip;
    private String path;
    private String userAgent;
    private int statusCode;

    @JsonIgnore
    public boolean isError() {
        return statusCode >= 400;
    }
}
