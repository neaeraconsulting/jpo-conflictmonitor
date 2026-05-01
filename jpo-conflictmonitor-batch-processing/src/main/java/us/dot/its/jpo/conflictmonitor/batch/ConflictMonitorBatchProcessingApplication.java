package us.dot.its.jpo.conflictmonitor.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(ConflictMonitorBatchProcessingProperties.class)
public class ConflictMonitorBatchProcessingApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConflictMonitorBatchProcessingApplication.class, args);
    }

}
