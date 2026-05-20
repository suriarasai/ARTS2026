package sg.edu.iss.tempdemo.configurations;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

/**
 * Kafka Consumer configuration — upgraded for Kafka 4.x / KRaft mode.
 *
 * <p><b>Key upgrade notes:</b>
 * <ul>
 *   <li><b>KIP-848 (Kafka 4.x)</b> — {@code GROUP_PROTOCOL_CONFIG = "consumer"} enables the
 *       next-generation incremental rebalance protocol. New group members receive partitions
 *       incrementally without forcing a full stop-the-world reassignment, which greatly reduces
 *       latency spikes during rolling restarts.</li>
 *   <li><b>KRaft mode</b> — ZooKeeper is removed from Kafka 4.0. The bootstrap-server address
 *       still points to the broker on port 9092; no {@code --zookeeper} flag is used anywhere.</li>
 *   <li><b>No ZooKeeper dependency</b> — All topic management (create/list/describe) must now
 *       use {@code --bootstrap-server} instead of the old {@code --zookeeper} flag.</li>
 * </ul>
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrapserver}")
    private String bootstrapServer;

    /**
     * Returns the raw consumer properties map used by both the factory beans below.
     * Centralising properties here avoids duplication and makes them easy to override
     * in integration tests via {@code @TestPropertySource}.
     */
    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,            "temp-groupid.group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,   "latest");

        // ── KIP-848: next-generation consumer group rebalance protocol ────────────
        // Available from kafka-clients 3.7+; stabilised and recommended in Kafka 4.x.
        // "consumer" = new incremental cooperative protocol.
        // "classic"  = old eager protocol (default if this key is absent).
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");  // KIP-848

        return props;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>>
            kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
