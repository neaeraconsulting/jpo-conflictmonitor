package us.dot.its.jpo.conflictmonitor.monitor.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Logs meter registration failures for diagnosing instrumentation problems.
 */
@Configuration
public class MetricsConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(MetricsConfiguration.class);

    @Bean
    MeterRegistryCustomizer<MeterRegistry> conflictMonitorMeterRegistryCustomizer() {
        return registry -> registry.config().onMeterRegistrationFailed((id, reason) ->
                logger.warn("Micrometer registration failed for {}: {}", id, reason));
    }
}
