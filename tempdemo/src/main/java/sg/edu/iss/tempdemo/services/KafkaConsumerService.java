package sg.edu.iss.tempdemo.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

/**
 * Kafka consumer service.
 *
 * <p>Listens on the configured Kafka topic ({@code kafka.topic}).  For every
 * message received that parses as a valid number, it forwards the value to all
 * WebSocket clients subscribed to {@code /topic/temperature}, which causes the
 * live Chart.js line chart to update in the browser.
 *
 * <p>Non-numeric messages are silently dropped — only temperature readings
 * (decimal or integer strings) are forwarded.
 */
@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final SimpMessagingTemplate template;

    @Autowired
    public KafkaConsumerService(SimpMessagingTemplate template) {
        this.template = template;
    }

    /**
     * Invoked by the Kafka listener container for every record on the topic.
     * Uses KIP-848 next-generation group protocol if configured in
     * {@link sg.edu.iss.tempdemo.configurations.KafkaConsumerConfig}.
     *
     * @param message raw string value from the Kafka record
     */
    @KafkaListener(topics = "${kafka.topic}")
    public void consume(@Payload String message) {
        if (isNumeric(message)) {
            log.debug("Forwarding temperature '{}' to WebSocket clients", message);
            template.convertAndSend("/topic/temperature", message);
        } else {
            log.debug("Ignoring non-numeric message: '{}'", message);
        }
    }

    /**
     * Returns {@code true} if {@code str} can be parsed as a {@code double}.
     * Used as a simple filter to ensure only numeric temperature values
     * are pushed to the chart.
     */
    private boolean isNumeric(String str) {
        if (str == null || str.isBlank()) return false;
        try {
            Double.parseDouble(str.trim());
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
