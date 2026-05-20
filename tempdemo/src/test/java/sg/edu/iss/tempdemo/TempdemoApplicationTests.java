package sg.edu.iss.tempdemo;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Smoke test — verifies the Spring context loads correctly against an
 * embedded Kafka broker.  {@code @EmbeddedKafka} spins up a KRaft-compatible
 * in-process Kafka instance so no external broker is needed during CI.
 */
@SpringBootTest(properties = {
        "kafka.bootstrapserver=${spring.embedded.kafka.brokers}",
        "kafka.topic=livetemperature"
})
@EmbeddedKafka(
        partitions = 1,
        topics     = { "livetemperature" }
)
@DirtiesContext
class TempdemoApplicationTests {

    @Test
    void contextLoads() {
        // If this test passes, all beans (Kafka consumer/producer configs,
        // WebSocket config, controllers, services) wired correctly.
    }
}
