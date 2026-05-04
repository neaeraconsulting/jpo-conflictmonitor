package us.dot.its.jpo.conflictmonitor.batch.scheduler.atspm_spat_validation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.SpringScheduledTask;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationTaskMetadata;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.RouteConfig;
import us.dot.its.jpo.conflictmonitor.batch.client.atspm.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.batch.client.atspm.AtspmToken;
import us.dot.its.jpo.conflictmonitor.batch.client.atspm.AtspmTokenService;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedControllerEvent;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.ControllerEventLog;

import java.time.*;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


@Slf4j
public class AtspmSpatValidationTask
        extends SpringScheduledTask<RouteConfig, AtspmSpatValidationParameters> {

    private final AtspmTokenService tokenService;
    private final AtspmClientService clientService;
    private final MongoTemplate mongoTemplate;


    public AtspmSpatValidationTask(
            RouteConfig taskMetadata,
            AtspmSpatValidationParameters parameters,
            AtspmTokenService tokenService,
            AtspmClientService clientService,
            Clock clock,
            MongoTemplate mongoTemplate) {
        super(taskMetadata, parameters, clock);
        this.tokenService = tokenService;
        this.clientService = clientService;
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void run() {
        log.info("Running AtspmSpatValidationTask with metadata: {}", taskMetadata);
        AtspmToken token = tokenService.token();
        log.info("Got token.");
        String authentication = clientService.authenticate();
        log.info("Got authentication response: {}", authentication);
        int routeId = taskMetadata.getRouteId();

        // Offset time to query by the grace period
        Duration gracePeriod = Duration.of(parameters.getGracePeriodOffset(), parameters.getGracePeriodOffsetUnits());
        Instant endInstant = clock.instant().minus(gracePeriod);

        // Get start and end time in local time zone
        LocalDateTime endTimeLocal = LocalDateTime.ofInstant(endInstant, parameters.getLocalTimeZone());
        Duration interval = Duration.of(parameters.getInterval(), parameters.getIntervalUnits());
        Instant startInstant = endInstant.minus(interval);
        LocalDateTime startTimeLocal = LocalDateTime.ofInstant(startInstant, parameters.getLocalTimeZone());

        if (!taskMetadata.enabledSignals()) {
            log.info("Route ID {} has no enabled signals, not getting event logs", routeId);
            return;
        }

        log.info("Getting event logs for route ID: {} between {} and {} local time ({})", routeId, startTimeLocal,
                endTimeLocal, parameters.getLocalTimeZone());
        List<ControllerEventLog> eventLogs = clientService.controllerEventLogs(startTimeLocal, endTimeLocal, routeId);
        log.info("Got eventLogs with {} items", eventLogs.size());

        var processedLog = new ProcessedControllerEventLog(routeId, startInstant, endInstant, eventLogs, clock, parameters.getLocalTimeZone());

        log.info("Processed event log has {} items", processedLog.size());

        if (!processedLog.getEvents().isEmpty()) {
            log.info("saving processed event log to mongo");
            mongoTemplate.insert(processedLog);
        }
    }
}
