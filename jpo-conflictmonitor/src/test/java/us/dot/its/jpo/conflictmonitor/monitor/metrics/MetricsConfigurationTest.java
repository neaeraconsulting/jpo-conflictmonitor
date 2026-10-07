package us.dot.its.jpo.conflictmonitor.monitor.metrics;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetricsConfigurationTest {
    @Test
    public void meterRegistrationFailureLogsMeterAndReason() {
        Logger logger = (Logger) LoggerFactory.getLogger(MetricsConfiguration.class);
        Level previousLevel = logger.getLevel();
        logger.setLevel(Level.WARN);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        try {
            new MetricsConfiguration().conflictMonitorMeterRegistryCustomizer().customize(registry);
            registry.counter("collision", "first", "value");
            registry.counter("collision", "second", "value");
            assertEquals(1, appender.list.size());
            String message = appender.list.getFirst().getFormattedMessage();
            assertTrue(message.contains("Micrometer registration failed for"));
            assertTrue(message.contains("collision"));
            assertTrue(message.contains("same set of tag keys"));
        } finally {
            registry.close();
            logger.detachAppender(appender);
            logger.setLevel(previousLevel);
            appender.stop();
        }
    }
}
