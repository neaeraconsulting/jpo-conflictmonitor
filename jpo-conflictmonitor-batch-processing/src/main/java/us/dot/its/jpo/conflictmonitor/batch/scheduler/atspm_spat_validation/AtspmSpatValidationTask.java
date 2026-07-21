package us.dot.its.jpo.conflictmonitor.batch.scheduler.atspm_spat_validation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;

import us.dot.its.jpo.conflictmonitor.batch.algorithms.SpringScheduledTask;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.RouteConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.SignalConfig;
import us.dot.its.jpo.conflictmonitor.batch.events.AtspmSpatPairEvent;
import us.dot.its.jpo.conflictmonitor.batch.events.AtspmSpatSignalGroupAlignmentEvent;
import us.dot.its.jpo.conflictmonitor.batch.events.AtspmSpatSignalGroupPairEvent;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.SignalGroupStatistics;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupIndicationLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupStateLog;
import us.dot.its.jpo.conflictmonitor.batch.mongo.ProcessedSpatCollectionUpdater;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmClientService;
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
    private final ProcessedSpatCollectionUpdater spatUpdater;


    public AtspmSpatValidationTask(
            RouteConfig routeConfig,
            AtspmSpatValidationParameters parameters,
            AtspmTokenService tokenService,
            AtspmClientService clientService,
            Clock clock,
            MongoTemplate mongoTemplate,
            AtspmSpatValidationService atspmSpatService,
            ProcessedSpatService spatService,
            ProcessedSpatCollectionUpdater spatUpdater) {
        super(routeConfig, parameters, clock);
        this.tokenService = tokenService;
        this.clientService = clientService;
        this.mongoTemplate = mongoTemplate;
        this.atspmSpatService = atspmSpatService;
        this.spatService = spatService;
        this.spatUpdater = spatUpdater;
    }

    @Override
    public void run() {
        log.info("Running AtspmSpatValidationTask");
        log.debug("metadata: {}", taskMetadata);
        tokenService.token();
        log.info("Got token.");
        String authentication = clientService.authenticate();
        log.info("Got authentication response: {}", authentication);
        int routeId = taskMetadata.getRouteId();

        // Update the ProcessedSpat Collection in Mongo
        spatUpdater.updateTimestamp();
        log.info("Updated ProcessedSpat");

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

        var processedLog = new ProcessedControllerEventLog(routeId, startInstant, endInstant, eventLogs, clock,
                parameters.getLocalTimeZone(), taskMetadata);

        log.info("Processed event log has {} items", processedLog.size());

        if (!processedLog.getSignalPhaseMap().isEmpty()) {
            log.info("saving processed event log to mongo");
            mongoTemplate.insert(processedLog);
        }

        // Get signal group alignment events
        List<AtspmSpatSignalGroupAlignmentEvent> alignmentEvents
                = atspmSpatService.atspmSpatSignalGroupAlignmentEvents(routeId, startInstant, endInstant);
        for (AtspmSpatSignalGroupAlignmentEvent event : alignmentEvents) {
            mongoTemplate.insert(event);
        }

        // Get atspm-spat validation logs
        var atspmSpatLogs = atspmSpatService.atspmSpatLogs(routeId, startInstant, endInstant);
        for (AtspmSpatPairLog log : atspmSpatLogs) {
            mongoTemplate.insert(log);

            if (log.getAtspmSpatPairs() != null && !log.getAtspmSpatPairs().isEmpty()) {
                var groupStatsMap = log.getSignalGroupStatistics();

                // Write one blended, intersection-level event when the overall (all signal
                // groups combined) percentage of paired ATSPM/SPAT events is below 90% for
                // any indication. Kept for compatibility with existing consumers of the
                // per-intersection CmAtspmSpatPairEvent collection. An indication with zero
                // transitions across every signal group in this window is skipped, same as
                // below, since there's nothing to evaluate - a blended count can only be
                // zero if every signal group's count for that indication is also zero.
                long totalGreenCount = groupStatsMap.values().stream().mapToLong(SignalGroupStatistics::greenCount).sum();
                long totalRedCount = groupStatsMap.values().stream().mapToLong(SignalGroupStatistics::redCount).sum();
                long totalYellowCount = groupStatsMap.values().stream().mapToLong(SignalGroupStatistics::yellowCount).sum();
                boolean logGreenBelowThreshold = totalGreenCount > 0 && log.getPercentGreenPaired() < 90.0;
                boolean logRedBelowThreshold = totalRedCount > 0 && log.getPercentRedPaired() < 90.0;
                boolean logYellowBelowThreshold = totalYellowCount > 0 && log.getPercentYellowPaired() < 90.0;
                if (logGreenBelowThreshold || logRedBelowThreshold || logYellowBelowThreshold) {
                    var pairEvent = AtspmSpatPairEvent.fromLog(log);
                    mongoTemplate.insert(pairEvent);
                }

                // Write an event to mongo for each signal group where any indication has less
                // than 90% paired. Checked per signal group (not blended across the whole
                // signal) so that one badly-diverging signal group isn't masked by other,
                // well-paired signal groups at the same intersection.
                // An indication with zero transitions for a signal group in this window (e.g. a
                // phase that didn't go red at all) is skipped rather than treated as a failing
                // 0% - there's nothing to evaluate, so it isn't evidence of a problem.
                for (SignalGroupStatistics signalGroupStats : groupStatsMap.values()) {
                    boolean greenBelowThreshold = signalGroupStats.greenCount() > 0 && signalGroupStats.percentGreenPaired() < 90.0;
                    boolean redBelowThreshold = signalGroupStats.redCount() > 0 && signalGroupStats.percentRedPaired() < 90.0;
                    boolean yellowBelowThreshold = signalGroupStats.yellowCount() > 0 && signalGroupStats.percentYellowPaired() < 90.0;
                    if (greenBelowThreshold || redBelowThreshold || yellowBelowThreshold) {
                        var groupPairEvent = AtspmSpatSignalGroupPairEvent.fromLog(log, signalGroupStats);
                        mongoTemplate.insert(groupPairEvent);
                    }
                }
            }
        }

        // Get the re-processed spat data for intersections on the route to save in Mongo
        RouteConfig routeConfig = parameters.findRouteConfig(routeId);
        for (SignalConfig signal : routeConfig.getSignals()) {
            final Integer intersectionId = signal.getIntersectionId();
            if (intersectionId == null || intersectionId <= 0) {
                String msg = String.format("Missing intersection id for signal %s", signal);
                log.warn(msg);
                continue;
            }
            SignalGroupStateLog spatStateLog = spatService.signalGroupLogs(intersectionId, startInstant, endInstant);
            log.info("Got spat state log for intersection {}", intersectionId);
            mongoTemplate.insert(spatStateLog);
            SignalGroupIndicationLog spatIndicationLog = spatService.signalGroupIndicationLogs(intersectionId, startInstant, endInstant);
            log.info("Got spat indication log for intersection {}", intersectionId);
            mongoTemplate.insert(spatIndicationLog);
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
