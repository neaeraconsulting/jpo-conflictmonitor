package us.dot.its.jpo.conflictmonitor.atspm;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import us.dot.its.jpo.conflictmonitor.atspm.client.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.atspm.client.AtspmToken;
import us.dot.its.jpo.conflictmonitor.atspm.client.AtspmTokenService;

import java.time.Instant;

@RestController
@RequestMapping(path = "/test", produces = MediaType.APPLICATION_JSON_VALUE)
@Slf4j
public class TestController {

    private final AtspmTokenService tokenService;
    private final AtspmClientService clientService;

    @Autowired
    public TestController(AtspmTokenService tokenService, AtspmClientService clientService) {
        this.tokenService = tokenService;
        this.clientService = clientService;
    }

    @GetMapping(path = "/token")
    public AtspmToken getToken() {
        return tokenService.token();
    }

    @GetMapping(path = "/authenticate")
    public String authenticate() {
        return clientService.authenticate();
    }

    @GetMapping(path = "/health")
    public Health health() {
        return new Health(true, String.format("%s", Instant.now()));
    }

    @ExceptionHandler(Throwable.class)
    public Health handleException(Throwable ex) {
        return new Health(false, ex.getClass().getName() + "\n" + ex.getMessage());
    }



    public record Health (boolean healthy, String message){}
}
