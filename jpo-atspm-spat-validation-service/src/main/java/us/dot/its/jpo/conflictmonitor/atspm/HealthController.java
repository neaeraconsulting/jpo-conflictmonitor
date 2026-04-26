package us.dot.its.jpo.conflictmonitor.atspm;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;

@RestController
@RequestMapping(path = "/health", produces = MediaType.APPLICATION_JSON_VALUE)
public class HealthController {

    @GetMapping
    public Health health() {
        return new Health(true, String.format("%s", Instant.now()));
    }

    @ExceptionHandler(Throwable.class)
    public Health handleException(Throwable ex) {
        return new Health(false, ex.getClass().getName() + "\n" + ex.getMessage());
    }

    public record Health (boolean healthy, String message){}
}
