package sg.edu.iss.tempdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the Live Temperature Demo application.
 *
 * <p>@SpringBootApplication already includes @EnableAutoConfiguration and
 * @ComponentScan, so those annotations are not needed separately.
 *
 * <p>Upgrade notes (Spring Boot 2.6 → 3.4 / Java 11 → 21):
 * <ul>
 *   <li>Requires Java 17+ at compile and runtime (we target Java 21 LTS).</li>
 *   <li>Jakarta EE 10 replaces javax.* namespace (jakarta.* everywhere).</li>
 *   <li>Spring Security 6, Spring Data 3, Hibernate 6 included if on classpath.</li>
 * </ul>
 */
@SpringBootApplication
public class TempdemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(TempdemoApplication.class, args);
    }
}
