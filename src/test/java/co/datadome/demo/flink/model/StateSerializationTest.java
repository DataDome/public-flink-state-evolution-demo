package co.datadome.demo.flink.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.junit.jupiter.api.Test;

/**
 * Guards the assumption the whole demo rests on: every type that ends up in state is serialized by
 * Flink's {@code PojoSerializer}, which supports adding and removing fields, and never by Kryo,
 * which does not.
 *
 * <p>If someone adds a field Flink cannot treat as a POJO field, this test fails here rather than
 * at savepoint restore time.
 */
class StateSerializationTest {

    private static <T> TypeSerializer<T> serializerFor(Class<T> type) {
        TypeInformation<T> typeInfo = TypeInformation.of(type);
        assertThat(typeInfo).as("%s must be recognised as a POJO", type.getSimpleName())
                .isInstanceOf(PojoTypeInfo.class);
        return typeInfo.createSerializer(new SerializerConfigImpl());
    }

    @Test
    void ipStatsIsSerializedAsAPojo() {
        assertThat(serializerFor(IpStats.class)).isInstanceOf(PojoSerializer.class);
    }

    @Test
    void httpRequestIsSerializedAsAPojo() {
        assertThat(serializerFor(HttpRequest.class)).isInstanceOf(PojoSerializer.class);
    }

    @Test
    void ruleIsSerializedAsAPojo() {
        assertThat(serializerFor(Rule.class)).isInstanceOf(PojoSerializer.class);
    }

    @Test
    void ruleMatchIsSerializedAsAPojo() {
        assertThat(serializerFor(RuleMatch.class)).isInstanceOf(PojoSerializer.class);
    }

    @Test
    void everyIpStatsFieldIsCoveredBySerialization() {
        PojoTypeInfo<IpStats> typeInfo = (PojoTypeInfo<IpStats>) TypeInformation.of(IpStats.class);
        assertThat(typeInfo.getFieldNames())
                .containsExactlyInAnyOrder(
                        "ip",
                        "sessionStartMs",
                        "lastSeenMs",
                        "totalCount",
                        "errorCount",
                        "distinctPathCount");
    }
}
