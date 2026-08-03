package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

import us.dot.its.jpo.conflictmonitor.batch.algorithms.ScheduledTaskAlgorithm;

public interface AtspmSpatValidationScheduledTaskAlgorithm extends AtspmSpatValidationAlgorithm,
        ScheduledTaskAlgorithm<AtspmSpatValidationParameters, RouteConfig>  {
}
