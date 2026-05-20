package sg.edu.iss.tempdemo.configurations;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * WebSocket / STOMP broker configuration.
 *
 * <p>The in-memory simple broker routes messages with prefix {@code /topic}.
 * The single STOMP endpoint {@code /live-temperature} is exposed with SockJS
 * fallback support for browsers that do not support native WebSockets.
 *
 * <p>Flow:
 * <pre>
 *   Browser ──SockJS/STOMP──▶ /live-temperature  (connect)
 *           ◀─── STOMP push ── /topic/temperature (subscribe)
 *
 *   Kafka consumer receives message
 *     → KafkaConsumerService.consume()
 *       → SimpMessagingTemplate.convertAndSend("/topic/temperature", value)
 *         → pushed to all subscribed browser clients
 * </pre>
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    /**
     * STOMP endpoint that browsers connect to.
     * SockJS is enabled so older browsers gracefully fall back to
     * HTTP long-polling or other transport mechanisms.
     */
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/live-temperature").withSockJS();
    }

    /**
     * Configures the simple (in-memory) message broker.
     * All destinations prefixed with "/topic" are routed through this broker
     * to the subscribed WebSocket clients.
     */
    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.enableSimpleBroker("/topic");
    }
}
