package sg.edu.iss.tempdemo.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import sg.edu.iss.tempdemo.services.KafkaProducerService;

import java.util.Map;

/**
 * REST controller for the Producer Panel.
 *
 * <p>Exposes a single endpoint that the browser calls via jQuery AJAX
 * when the user clicks "Send Temperature" in the Producer Panel.
 *
 * <p>Flow:
 * <pre>
 *   Browser form (home.html)
 *     ──POST /api/temperature { "value": "36.5" }──▶ ProducerController
 *       ── KafkaProducerService.send("36.5")
 *         ── Kafka topic "livetemperature"
 *           ── KafkaConsumerService.consume("36.5")
 *             ── SimpMessagingTemplate → /topic/temperature
 *               ── Browser WebSocket chart update
 * </pre>
 */
@RestController
@RequestMapping("/api")
public class ProducerController {

    private final KafkaProducerService producerService;

    @Autowired
    public ProducerController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    /**
     * Accepts a temperature value from the browser and publishes it to Kafka.
     *
     * <p>Request body: {@code { "value": "36.5" }}
     *
     * @param body JSON map containing the "value" key
     * @return 200 OK with a confirmation message, or 400 if value is missing/invalid
     */
    @PostMapping("/temperature")
    public ResponseEntity<String> publishTemperature(@RequestBody Map<String, String> body) {
        String value = body.get("value");

        if (value == null || value.isBlank()) {
            return ResponseEntity.badRequest().body("Missing 'value' in request body.");
        }

        try {
            Double.parseDouble(value.trim());  // validate numeric before sending
        } catch (NumberFormatException e) {
            return ResponseEntity.badRequest().body("Value must be numeric, got: " + value);
        }

        producerService.send(value.trim());
        return ResponseEntity.ok("Published: " + value.trim());
    }
}
