package us.dot.its.jpo.conflictmonitor.batch.time;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Configure custom clock for testing and historical reporting
 */
@Slf4j
@Data
@Configuration
@ConfigurationProperties(prefix = "cm.batch.clock")
public class ClockConfig {

    private boolean offset;
    private Instant startTimestamp;

    @Bean
    @Primary
    public Clock customClock() {
        log.info("Starting clock. offset: {}, Start timestamp: {}, ", offset, startTimestamp);
        if (!offset) {
            Clock clock = Clock.systemUTC();
            log.info("Starting normal clock at UTC system time {}", clock.instant());
            return clock;
        } else {
            Clock baseClock = Clock.systemUTC();
            Duration offsetDuration = Duration.between(baseClock.instant(), startTimestamp);
            Clock offsetClock = Clock.offset(baseClock, offsetDuration);
            log.info("Traveling in time: starting clock at UTC time {}, offset {} from system time",
                    offsetClock.instant(), offsetDuration);
            return offsetClock;
        }
    }
}
