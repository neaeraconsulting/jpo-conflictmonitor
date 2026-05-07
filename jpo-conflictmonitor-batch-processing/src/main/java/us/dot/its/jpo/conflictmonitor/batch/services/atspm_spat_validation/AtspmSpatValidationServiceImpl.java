package us.dot.its.jpo.conflictmonitor.batch.services.atspm_spat_validation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.RouteConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.SignalConfig;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.ControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupIndicationLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatSignalIndication;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.TimestampedIndication;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.batch.services.spat.ProcessedSpatService;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
public class AtspmSpatValidationServiceImpl implements AtspmSpatValidationService{

    private final AtspmSpatValidationParameters parameters;
    private final AtspmClientService atspmClientService;
    private final ProcessedSpatService spatService;
    private final Clock clock;

    @Autowired
    public AtspmSpatValidationServiceImpl(AtspmSpatValidationParameters parameters,
            AtspmClientService atspmClientService, ProcessedSpatService spatService, Clock clock) {
        this.parameters = parameters;
        this.atspmClientService = atspmClientService;
        this.spatService = spatService;
        this.clock = clock;
    }

    @Override
    public AtspmSpatPairLog atpsmSpatLogs(int routeId, Instant startTime, Instant endTime) {

        // Get Route Config
        RouteConfig routeConfig = parameters.findRouteConfig(routeId);
        if (!routeConfig.enabledSignals()) {
            log.warn("No enabled signals for route {}, not doing this", routeId);
            return new AtspmSpatPairLog();
        }

        // Get ATSPM Events for route
        LocalDateTime localStartTime = LocalDateTime.ofInstant(startTime, parameters.getLocalTimeZone());
        LocalDateTime localEndTime = LocalDateTime.ofInstant(endTime, parameters.getLocalTimeZone());
        ProcessedControllerEventLog atspmLog = atspmClientService.processedEventLogs(localStartTime, localEndTime, routeId);
        log.info("Got eventLogs with {} items", atspmLog.size());
        ProcessedControllerEventLog.SignalPhaseMap astpmEventMap = atspmLog.getSignalPhaseMap();

        // Get Spats for each intersection/signal on the route
        for (SignalConfig signal : routeConfig.getSignals()) {
            final Integer intersectionId = signal.getIntersectionId();
            if (intersectionId == null) {
                log.warn("Missing intersection id for signal {}", signal);
                continue;
            }
            SignalGroupIndicationLog spatLog = spatService.signalGroupIndicationLogs(intersectionId, startTime, endTime);
            log.info("Got spatLog for signal {}", signal);

            final String signalId = signal.getSignalId();

            SignalGroupIndicationLog.SignalGroupIndicationMap signalGroupMap = spatLog.getIndicationsMap();
            for (final Integer signalGroup : signalGroupMap.keySet()) {
                List<TimestampedIndication> indications = signalGroupMap.getIndications(signalGroup);
                for (TimestampedIndication indication : indications) {
                    Instant spatTimestamp = indication.getTimestamp();
                    SpatSignalIndication spatColor = indication.getIndication();
                }
            }
        }


        return  null;


    }
}
