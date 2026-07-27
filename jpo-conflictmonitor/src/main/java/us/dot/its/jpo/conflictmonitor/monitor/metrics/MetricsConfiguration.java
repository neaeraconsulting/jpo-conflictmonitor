package us.dot.its.jpo.conflictmonitor.monitor.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Keeps the Prometheus registry scrape-safe when Kafka / many Streams apps are present.
 */
@Configuration
public class MetricsConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(MetricsConfiguration.class);

    @Bean
    MeterRegistryCustomizer<MeterRegistry> conflictMonitorMeterRegistryCustomizer() {
        return registry -> {
            // Deny any Kafka client/streams meters that slip through (variable tag keys break scrape).
            registry.config().meterFilter(MeterFilter.deny(id -> {
                String name = id.getName();
                return name.equals("kafka") || name.startsWith("kafka.");
            }));
            registry.config().onMeterRegistrationFailed((id, reason) ->
                    logger.warn("Micrometer registration failed for {}: {}", id, reason));
        };
    }
}
