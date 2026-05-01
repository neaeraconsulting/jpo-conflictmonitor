package us.dot.its.jpo.conflictmonitor.batch.controllers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import us.dot.its.jpo.conflictmonitor.batch.ConflictMonitorBatchProcessingProperties;

@Slf4j
@Controller
public class ConflictMonitorBatchProcessingController {


    final ConflictMonitorBatchProcessingProperties properties;

    @Autowired
    public ConflictMonitorBatchProcessingController(ConflictMonitorBatchProcessingProperties properties) {
        log.info("Starting {}", this.getClass().getSimpleName());

        this.properties = properties;

        // Start ATSPM SPAT Comparison algorithm
        String atspmSpatValidationAlgorithmName = properties.getAtspmSpatValidationAlgorithm();
        var atspmSpatValidationAlgorithmFactory = properties.getAtspmSpatValidationAlgorithmFactory();
        var atspmSpatValidationAlgorithm = atspmSpatValidationAlgorithmFactory.getAlgorithm(atspmSpatValidationAlgorithmName);
        atspmSpatValidationAlgorithm.start();
        log.info("AtspmSpatValidationAlgorithm started.");

        log.info("Batch processing algorithms started started.");
    }

}
