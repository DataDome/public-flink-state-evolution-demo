package co.datadome.demo.flink.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads JSON records of that type off a Kafka topic.
 *
 * <p>A record that cannot be parsed is logged and skipped rather than failing the job, because
 * during a live demo the rules topic is fed by hand.
 *
 * @param <T> the type to deserialize into
 */
public final class JsonDeserializer<T> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(JsonDeserializer.class);

    private final Class<T> type;

    /** Not serializable, so it is rebuilt on the task manager in {@link #open}. */
    private transient ObjectMapper mapper;

    public JsonDeserializer(Class<T> type) {
        this.type = type;
    }

    @Override
    public void open(InitializationContext context) {
        mapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        try {
            return mapper.readValue(message, type);
        } catch (IOException e) {
            LOG.warn("Skipping malformed {} record: {}", type.getSimpleName(), new String(message), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(type);
    }
}
