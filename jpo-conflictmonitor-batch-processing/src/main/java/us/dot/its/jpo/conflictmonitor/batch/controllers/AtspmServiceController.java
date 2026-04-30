package us.dot.its.jpo.conflictmonitor.batch.controllers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationAlgorithm;

@Slf4j
@Controller
public class AtspmServiceController {


    final AtspmSpatValidationAlgorithm atspmSpatValidationAlgorithm;

    @Autowired
    public AtspmServiceController(AtspmSpatValidationAlgorithm atspmSpatValidationAlgorithm) {

        this.atspmSpatValidationAlgorithm = atspmSpatValidationAlgorithm;
        atspmSpatValidationAlgorithm.start();
        log.info("AtspmServiceController started.");
    }

}
