package co.datadome.demo.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Objects;

/**
 * A heavily simplified HTTP request, as read from the input Kafka topic.
 *
 * <p>This class is mutable, and has a public no-argument constructor, because Flink only recognises
 * a type as a POJO (and therefore only uses {@code PojoSerializer} for it) under those conditions.
 * Instances should be treated as immutable once handed to Flink.
 */
public final class HttpRequest {

    /** Event time of that request. */
    private long timestampMs;

    private String ip;
    private String path;
    private String userAgent;
    private int statusCode;

    /** Required by Flink's POJO serializer. */
    public HttpRequest() {}

    public HttpRequest(long timestampMs, String ip, String path, String userAgent, int statusCode) {
        this.timestampMs = timestampMs;
        this.ip = ip;
        this.path = path;
        this.userAgent = userAgent;
        this.statusCode = statusCode;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public void setTimestampMs(long timestampMs) {
        this.timestampMs = timestampMs;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HttpRequest that)) {
            return false;
        }
        return timestampMs == that.timestampMs
                && statusCode == that.statusCode
                && Objects.equals(ip, that.ip)
                && Objects.equals(path, that.path)
                && Objects.equals(userAgent, that.userAgent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestampMs, ip, path, userAgent, statusCode);
    }

    @Override
    public String toString() {
        return "HttpRequest{timestampMs=%d, ip='%s', path='%s', userAgent='%s', statusCode=%d}"
                .formatted(timestampMs, ip, path, userAgent, statusCode);
    }
}
