package co.datadome.demo.flink.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Writes records of that type to a Kafka topic as JSON.
 *
 * @param <T> the type to serialize
 */
public final class JsonSerializer<T> implements SerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    /** Not serializable, so it is rebuilt on the task manager in {@link #open}. */
    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(T element) {
        try {
            return mapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            // Nothing sensible to fall back on: a record we cannot serialize is a bug, not bad input.
            throw new IllegalStateException("Could not serialize " + element, e);
        }
    }
}
