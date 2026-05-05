package us.dot.its.jpo.conflictmonitor.batch.time;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.time.Clock;
import java.time.Instant;

/**
 * Configure custom clock for testing and historical reporting
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "cm.batch.clock")
public class ClockConfig {

    private boolean offset;
    private Instant startTimestamp;

    @Bean
    @Primary
    public Clock customClock() {
        return Clock.systemUTC();
    }
}
