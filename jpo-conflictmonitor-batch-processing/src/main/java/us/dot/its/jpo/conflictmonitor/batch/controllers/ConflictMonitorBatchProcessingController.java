package us.dot.its.jpo.conflictmonitor.batch.controllers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.batch.ConflictMonitorBatchProcessingProperties;

/**
 * Starts the configured batch algorithms after the application context has finished
 * loading. Runs as an ApplicationRunner (rather than eagerly in a bean constructor) so
 * that loading the Spring context - e.g. in tests - doesn't by itself start live
 * scheduled tasks against real external services. Can be disabled entirely via
 * cm.batch.scheduler.enabled, e.g. for tests.
 */
@Slf4j
@Component
public class ConflictMonitorBatchProcessingController implements ApplicationRunner {

    private final ConflictMonitorBatchProcessingProperties properties;
    private final boolean enabled;

    @Autowired
    public ConflictMonitorBatchProcessingController(
            ConflictMonitorBatchProcessingProperties properties,
            @Value("${cm.batch.scheduler.enabled:true}") boolean enabled) {
        this.properties = properties;
        this.enabled = enabled;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!enabled) {
            log.info("cm.batch.scheduler.enabled is false, not starting batch algorithms.");
            return;
        }

        log.info("Starting batch processing algorithms");

        // Start ATSPM SPAT Comparison algorithm
        String atspmSpatValidationAlgorithmName = properties.getAtspmSpatValidationAlgorithm();
        var atspmSpatValidationAlgorithmFactory = properties.getAtspmSpatValidationAlgorithmFactory();
        var atspmSpatValidationAlgorithm = atspmSpatValidationAlgorithmFactory.getAlgorithm(atspmSpatValidationAlgorithmName);
        atspmSpatValidationAlgorithm.start();
        log.info("AtspmSpatValidationAlgorithm started.");

        log.info("Batch processing algorithms started.");
    }
}
