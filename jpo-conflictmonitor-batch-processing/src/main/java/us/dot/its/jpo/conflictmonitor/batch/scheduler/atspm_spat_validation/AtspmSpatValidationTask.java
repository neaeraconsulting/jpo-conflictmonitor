package us.dot.its.jpo.conflictmonitor.batch.scheduler.atspm_spat_validation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.SpringScheduledTask;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationAlgorithm;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.batch.client.atspm.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.batch.client.atspm.AtspmTokenService;

import java.time.*;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

import static us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationConstants.DEFAULT_ATSPM_SPAT_VALIDATION_ALGORITHM;

@Slf4j
@Component(DEFAULT_ATSPM_SPAT_VALIDATION_ALGORITHM)
public class AtspmSpatValidationTask
        extends SpringScheduledTask<AtspmSpatValidationParameters>
        implements AtspmSpatValidationAlgorithm {

    private final AtspmTokenService tokenService;
    private final AtspmClientService clientService;
    private final Clock clock;


    @Autowired
    public AtspmSpatValidationTask(
            AtspmSpatValidationParameters parameters,
            ThreadPoolTaskScheduler taskScheduler, AtspmTokenService tokenService,
            AtspmClientService clientService,
            Clock clock) {
        super(taskScheduler);
        this.tokenService = tokenService;
        this.clientService = clientService;
        this.parameters = parameters;
        this.interval = Duration.of(parameters.getInterval(), parameters.getIntervalUnits());
        log.info("Interval {}", interval);
        this.clock = clock;

        // Start at top of the specified unit (e.g. top of the hour if interval units are hours.)
        Instant now = clock.instant();
        this.startTime = switch (parameters.getIntervalUnits()) {
            case ChronoUnit.HOURS -> now.truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS);
            case ChronoUnit.MINUTES -> now.truncatedTo(ChronoUnit.MINUTES).plus(1, ChronoUnit.MINUTES);
            case ChronoUnit.SECONDS -> now.truncatedTo(ChronoUnit.SECONDS).plus(1, ChronoUnit.SECONDS);
            default -> now;
        };
        log.info("Start time {}", startTime);
    }

    @Override
    public void run() {
        log.info("Tick");
    }
}
