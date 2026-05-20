package sg.edu.iss.tempdemo.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Service responsible for publishing temperature values to a Kafka topic.
 *
 * <p>Injected into {@link sg.edu.iss.tempdemo.controllers.ProducerController}
 * and called whenever the user submits a value from the Producer Panel.
 *
 * <p><b>KIP-1118 note (Kafka 4.1+):</b>
 * The {@code send()} callback below intentionally does NOT call
 * {@code kafkaTemplate.flush()}.  Calling flush() inside a send() callback
 * deadlocks the producer network thread in Kafka 4.1+ and will throw an
 * exception immediately.  Flush is only safe to call from outside the callback.
 */
@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic}")
    private String topic;

    @Autowired
    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes a temperature value string to the configured Kafka topic.
     *
     * @param value numeric temperature string (e.g. "36.5")
     */
    public void send(String value) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, value);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish '{}' to topic '{}': {}", value, topic, ex.getMessage());
            } else {
                log.debug("Published '{}' to topic '{}' partition {} offset {}",
                        value, topic,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
            // ⚠ Do NOT call kafkaTemplate.flush() here — KIP-1118 deadlock rule.
        });
    }
}
