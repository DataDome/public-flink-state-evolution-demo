package co.datadome.demo.flink.tools;

import co.datadome.demo.flink.model.HttpRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Produces a stream of fake HTTP requests into Kafka, so that the job has something to chew on.
 *
 * <p>Most of the traffic is unremarkable. One IP address behaves like a credential stuffing bot:
 * it hammers a single path and collects failures, so it crosses the thresholds of the demo rules
 * within a few seconds.
 *
 * <p>Runs until interrupted.
 */
public final class DemoDataGenerator {

    private static final List<String> NORMAL_IPS =
            List.of("10.0.0.10", "10.0.0.11", "10.0.0.12", "10.0.0.13");

    private static final String ABUSIVE_IP = "10.0.0.66";

    private static final List<String> NORMAL_PATHS =
            List.of("/", "/search", "/product", "/cart", "/help");

    private static final List<String> USER_AGENTS =
            List.of("Mozilla/5.0 (Macintosh)", "Mozilla/5.0 (Windows NT 10.0)", "curl/8.5.0");

    /**
     * Requests sent per second, across all IP addresses.
     */
    private static final int REQUESTS_PER_SECOND = 20;

    /**
     * Share of the traffic coming from the abusive IP address.
     */
    private static final double ABUSIVE_SHARE = 0.3;

    private DemoDataGenerator() {
    }

    public static void main(String[] args) throws Exception {
        String brokers = argument(args, "--bootstrap-servers", "localhost:9092");
        String topic = argument(args, "--topic", "http-requests");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        ObjectMapper mapper = new ObjectMapper();
        long pauseMs = Duration.ofSeconds(1).toMillis() / REQUESTS_PER_SECOND;

        System.out.printf("Producing ~%d requests/s to %s on %s. Ctrl-C to stop.%n",
                REQUESTS_PER_SECOND, topic, brokers);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (!Thread.currentThread().isInterrupted()) {
                HttpRequest request = nextRequest();
                producer.send(
                        new ProducerRecord<>(
                                topic, request.getIp(), mapper.writeValueAsString(request)));
                Thread.sleep(pauseMs);
            }
        }
    }

    private static HttpRequest nextRequest() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        long now = System.currentTimeMillis();

        if (random.nextDouble() < ABUSIVE_SHARE) {
            // Always the same path, mostly rejected: high total and error counts, one distinct path.
            int statusCode = random.nextInt(10) < 8 ? 403 : 200;
            return new HttpRequest(now, ABUSIVE_IP, "/login", "curl/8.5.0", statusCode);
        } else {
            String ip = NORMAL_IPS.get(random.nextInt(NORMAL_IPS.size()));
            String path = NORMAL_PATHS.get(random.nextInt(NORMAL_PATHS.size()));
            String userAgent = USER_AGENTS.get(random.nextInt(USER_AGENTS.size()));
            int statusCode = random.nextInt(20) == 0 ? 500 : 200;
            return new HttpRequest(now, ip, path, userAgent, statusCode);
        }
    }

    private static String argument(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}
