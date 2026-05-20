package sg.edu.iss.tempdemo.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Serves the main two-panel web page.
 *
 * <p>The page at {@code /home} contains:
 * <ul>
 *   <li><b>Producer Panel</b> — a Bootstrap card with a numeric input and a
 *       "Send" button. Submitting the form posts the value via AJAX to
 *       {@code POST /api/temperature}, which publishes it to Kafka.</li>
 *   <li><b>Consumer Panel</b> — a Chart.js 4.x line chart that updates in
 *       real-time as Kafka messages arrive via the STOMP/WebSocket channel.</li>
 * </ul>
 */
@Controller
@RequestMapping("/")
public class HomeController {

    @GetMapping("/home")
    public String home(Model model) {
        return "home";  // resolves to src/main/resources/templates/home.html
    }
}
