package sg.edu.iss.tempdemo.configurations;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Kafka Producer configuration — new in this upgrade to support the
 * in-browser Producer Panel.
 *
 * <p>The producer panel lets a user type a temperature value into the web UI.
 * The browser sends that value to a REST endpoint ({@code POST /api/temperature}),
 * which delegates to {@link sg.edu.iss.tempdemo.services.KafkaProducerService}
 * which publishes the message to the Kafka topic.  The consumer (on the same or
 * any other instance) picks it up and pushes it to the WebSocket chart.
 *
 * <p><b>KIP-1118 reminder (Kafka 4.1+):</b> Never call {@code KafkaProducer.flush()}
 * from inside a {@code send()} callback — this will now throw immediately instead of
 * silently deadlocking the producer network thread. Always call {@code flush()} from
 * outside the callback.
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${kafka.bootstrapserver}")
    private String bootstrapServer;

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Balanced durability: wait for leader ack only (good for demo; use "all" in production)
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return props;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    /**
     * KafkaTemplate is the primary Kafka abstraction for sending messages.
     * Injected into {@link sg.edu.iss.tempdemo.services.KafkaProducerService}.
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
