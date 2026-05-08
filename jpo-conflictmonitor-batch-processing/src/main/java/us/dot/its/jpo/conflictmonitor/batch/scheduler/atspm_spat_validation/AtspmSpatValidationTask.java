package us.dot.its.jpo.conflictmonitor.batch.scheduler.atspm_spat_validation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.SpringScheduledTask;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.RouteConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.SignalConfig;
import us.dot.its.jpo.conflictmonitor.batch.events.AtspmSpatSignalGroupAlignmentEvent;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupIndicationLog;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmToken;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmTokenService;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.ControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm_spat_validation.AtspmSpatValidationService;
import us.dot.its.jpo.conflictmonitor.batch.services.spat.ProcessedSpatService;

import java.time.*;
import java.util.List;


@Slf4j
public class AtspmSpatValidationTask
        extends SpringScheduledTask<RouteConfig, AtspmSpatValidationParameters> {

    private final AtspmTokenService tokenService;
    private final AtspmClientService clientService;
    private final MongoTemplate mongoTemplate;
    private final AtspmSpatValidationService atspmSpatService;
    private final ProcessedSpatService spatService;


    public AtspmSpatValidationTask(
            RouteConfig taskMetadata,
            AtspmSpatValidationParameters parameters,
            AtspmTokenService tokenService,
            AtspmClientService clientService,
            Clock clock,
            MongoTemplate mongoTemplate,
            AtspmSpatValidationService atspmSpatService,
            ProcessedSpatService spatService) {
        super(taskMetadata, parameters, clock);
        this.tokenService = tokenService;
        this.clientService = clientService;
        this.mongoTemplate = mongoTemplate;
        this.atspmSpatService = atspmSpatService;
        this.spatService = spatService;
    }

    @Override
    public void run() {
        log.info("Running AtspmSpatValidationTask");
        log.debug("metadata: {}", taskMetadata);
        AtspmToken token = tokenService.token();
        log.info("Got token.");
        String authentication = clientService.authenticate();
        log.info("Got authentication response: {}", authentication);
        int routeId = taskMetadata.getRouteId();

        // Offset time to query by the grace period
        Duration gracePeriod = Duration.of(parameters.getGracePeriodOffset(), parameters.getGracePeriodOffsetUnits());
        final Instant endInstant = clock.instant().minus(gracePeriod);

        // Get start and end time in local time zone
        LocalDateTime endTimeLocal = LocalDateTime.ofInstant(endInstant, parameters.getLocalTimeZone());
        Duration interval = Duration.of(parameters.getInterval(), parameters.getIntervalUnits());
        final Instant startInstant = endInstant.minus(interval);
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

        if (!processedLog.getSignalPhaseMap().isEmpty()) {
            log.info("saving processed event log to mongo");
            mongoTemplate.insert(processedLog);
        }

        // Get signal group alignment events
        List<AtspmSpatSignalGroupAlignmentEvent> alignmentEvents = atspmSpatService.atspmSpatSignalGroupAlignmentEvents(routeId, startInstant, endInstant);
        for (AtspmSpatSignalGroupAlignmentEvent event : alignmentEvents) {
            mongoTemplate.insert(event);
        }

        // Get atspm-spat validation logs
        var atspmSpatLogs = atspmSpatService.atpsmSpatLogs(routeId, startInstant, endInstant);
        for (AtspmSpatPairLog log : atspmSpatLogs) {
            mongoTemplate.insert(log);
        }

        // Get the re-processed spat data for intersections on the route to save in Mongo
        // TODO consolidate this
        RouteConfig routeConfig = parameters.findRouteConfig(routeId);
        for (SignalConfig signal : routeConfig.getSignals()) {
            final Integer intersectionId = signal.getIntersectionId();
            if (intersectionId == null || intersectionId <= 0) {
                String msg = String.format("Missing intersection id for signal %s", signal);
                log.warn(msg);
                continue;
            }
            SignalGroupIndicationLog spatLog = spatService.signalGroupIndicationLogs(intersectionId, startInstant, endInstant);
            log.info("Got spatLog for signal {}", signal);
            mongoTemplate.insert(spatLog);
        }

        // Get signal metadata and save to mongo
        for (SignalConfig signal : routeConfig.getSignals()) {
            var processedSignalConfig = clientService.processedSignalConfig(signal.getSignalId());
            if (processedSignalConfig != null) {
                mongoTemplate.insert(processedSignalConfig);
            }
        }

    }
}
