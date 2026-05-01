package us.dot.its.jpo.conflictmonitor.batch.scheduler.atspm_spat_validation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.SpringScheduledTask;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.SpringTaskScheduler;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationAlgorithm;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationTaskMetadata;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationConstants.DEFAULT_ATSPM_SPAT_VALIDATION_ALGORITHM;

@Slf4j
@Component(DEFAULT_ATSPM_SPAT_VALIDATION_ALGORITHM)
public class AtspmSpatValidationTaskScheduler
        extends SpringTaskScheduler<AtspmSpatValidationParameters, AtspmSpatValidationTaskMetadata, AtspmSpatValidationTask>
        implements AtspmSpatValidationAlgorithm {


    @Override
    protected AtspmSpatValidationTask createTask(AtspmSpatValidationTaskMetadata atspmSpatValidationTaskMetadata, AtspmSpatValidationParameters atspmSpatValidationParameters) {
        return null;
    }

    @Override
    public void run() {

    }

    @Override
    public void setInterval(int interval) {

    }

    @Override
    public void setTaskStartTimeStagger(int taskStartTimeStagger) {

    }
}
