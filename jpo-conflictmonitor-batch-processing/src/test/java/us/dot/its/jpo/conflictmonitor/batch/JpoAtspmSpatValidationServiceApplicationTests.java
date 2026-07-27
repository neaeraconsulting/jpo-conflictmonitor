package us.dot.its.jpo.conflictmonitor.batch;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "cm.batch.scheduler.enabled=false")
class JpoAtspmSpatValidationServiceApplicationTests {

    @Test
    void contextLoads() {
    }

}
