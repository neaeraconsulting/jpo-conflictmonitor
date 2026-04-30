package us.dot.its.jpo.conflictmonitor.atspm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class JpoAtspmSpatValidationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(JpoAtspmSpatValidationServiceApplication.class, args);
    }

}
