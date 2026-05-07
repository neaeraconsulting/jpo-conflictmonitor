package us.dot.its.jpo.conflictmonitor.batch.services.atspm_spat_validation;

import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.RouteConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.SignalConfig;
import us.dot.its.jpo.conflictmonitor.batch.events.AtspmSpatSignalGroupAlignmentEvent;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.EventCode;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedControllerEvent;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.ControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPair;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupIndicationLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatSignalIndication;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.TimestampedIndication;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.batch.services.spat.ProcessedSpatService;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

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
    public List<AtspmSpatPairLog> atpsmSpatLogs(int routeId, Instant startTime, Instant endTime) {
        List<AtspmSpatPairLog> logs = new ArrayList<>();

        // Get Route Config
        RouteConfig routeConfig = parameters.findRouteConfig(routeId);
        if (!routeConfig.enabledSignals()) {
            log.warn("No enabled signals for route {}, not doing this", routeId);
            return new ArrayList<>();
        }

        // Get ATSPM Events for route
        LocalDateTime localStartTime = LocalDateTime.ofInstant(startTime, parameters.getLocalTimeZone());
        LocalDateTime localEndTime = LocalDateTime.ofInstant(endTime, parameters.getLocalTimeZone());
        ProcessedControllerEventLog atspmLog = atspmClientService.processedEventLogs(localStartTime, localEndTime, routeId);
        log.info("Got eventLogs with {} items", atspmLog.size());
        ProcessedControllerEventLog.SignalPhaseMap signalPhaseMap = atspmLog.getSignalPhaseMap();

        // Get Spats for each intersection/signal on the route
        for (SignalConfig signal : routeConfig.getSignals()) {

            final Integer intersectionId = signal.getIntersectionId();
            final String signalId = signal.getSignalId();

            AtspmSpatPairLog pairLog = new AtspmSpatPairLog();
            pairLog.setRouteId(routeId);

            pairLog.setSignalId(signalId);
            pairLog.setStartTime(startTime);
            pairLog.setEndTime(endTime);
            pairLog.setAtspmSpatPairs(new ArrayList<>());
            logs.add(pairLog);

            if (intersectionId == null) {
                String msg = String.format("Missing intersection id for signal %s", signal);
                pairLog.setError(msg);
                log.warn(msg);
                continue;
            }

            pairLog.setIntersectionId(intersectionId);

            SignalGroupIndicationLog spatLog = spatService.signalGroupIndicationLogs(intersectionId, startTime, endTime);
            log.info("Got spatLog for signal {}", signal);


            if (!signalPhaseMap.containsKey(signalId)) {
                String msg = String.format("ATSPM Signal phase map has no entries for signalId {}", signalId);
                pairLog.setError(msg);
                log.warn(msg);
                continue;
            }
            final ProcessedControllerEventLog.PhaseMap phaseMap = signalPhaseMap.getPhaseMap(signalId);
            SignalGroupIndicationLog.SignalGroupIndicationMap signalGroupMap = spatLog.getIndicationsMap();



            for (final Integer signalGroup : signalGroupMap.keySet()) {
                List<TimestampedIndication> indications = signalGroupMap.getIndications(signalGroup);

                for (TimestampedIndication indication : indications) {
                    final Instant spatTimestamp = indication.getTimestamp();
                    final SpatSignalIndication spatIndication = indication.getIndication();
                    final EventCode eventCode = EventCode.fromSpatIndication(spatIndication);
                    AtspmSpatPair pair = new  AtspmSpatPair();
                    pair.setSpatTimestamp(spatTimestamp);
                    pair.setSpatIndication(spatIndication);
                    pair.setSpatMovementPhaseState(indication.getMovementPhaseState());
                    pair.setSpatSignalGroupId(signalGroup);
                    var eventResult
                            = phaseMap.findEventInWindow(signalGroup, eventCode, spatTimestamp, Duration.ofSeconds(3));
                    if (eventResult.event() != null) {
                        ProcessedControllerEvent event = eventResult.event();
                        pair.setAtspmTimestamp(event.getTimestamp());
                        pair.setAtspmEventCode(event.getEventCode());
                        pair.setAtspmPrimaryPhase(event.getPhase());
                        pair.setPaired(eventResult.paired());
                    } else {
                        pair.setPaired(false);
                    }
                    pairLog.getAtspmSpatPairs().add(pair);
                }
            }

        }
        return logs;
    }

    @Override
    public List<AtspmSpatSignalGroupAlignmentEvent> atspmSpatSignalGroupAlignmentEvents(int routeId, Instant startTime, Instant endTime) {

        var result = new ArrayList<AtspmSpatSignalGroupAlignmentEvent>();

        // Get Route Config
        RouteConfig routeConfig = parameters.findRouteConfig(routeId);
        if (!routeConfig.enabledSignals()) {
            log.warn("No enabled signals for route {}, not doing this", routeId);
            return new ArrayList<>();
        }

        // Get ATSPM Events for route
        LocalDateTime localStartTime = LocalDateTime.ofInstant(startTime, parameters.getLocalTimeZone());
        LocalDateTime localEndTime = LocalDateTime.ofInstant(endTime, parameters.getLocalTimeZone());
        ProcessedControllerEventLog atspmLog = atspmClientService.processedEventLogs(localStartTime, localEndTime, routeId);
        log.info("Got eventLogs with {} items", atspmLog.size());
        ProcessedControllerEventLog.SignalPhaseMap signalPhaseMap = atspmLog.getSignalPhaseMap();
        Multimap<String, Integer> phaseMultimap = MultimapBuilder.hashKeys().arrayListValues().build();
        for (String signalId : signalPhaseMap.keySet()) {
            ProcessedControllerEventLog.PhaseMap phaseMap = signalPhaseMap.getPhaseMap(signalId);
            for (final Integer phase : phaseMap.keySet()) {
                List<ProcessedControllerEvent> eventList = phaseMap.getEventList(phase);
                for (final ProcessedControllerEvent event : eventList) {
                    phaseMultimap.put(signalId, event.getPhase());
                }
            }
        }

        // Get Spats for each intersection/signal on the route
        for (SignalConfig signal : routeConfig.getSignals()) {

            final String signalId = signal.getSignalId();

            final Integer intersectionId = signal.getIntersectionId();
            if (intersectionId == null) {
                String msg = String.format("Missing intersection id for signal %s", signal);
                log.warn(msg);
                continue;
            }

            SignalGroupIndicationLog spatLog = spatService.signalGroupIndicationLogs(intersectionId, startTime, endTime);
            log.info("Got spatLog for signal {}", signal);

            Set<Integer> signalGroupSet = spatLog.getIndicationsMap().keySet();
            var alignmentEvent = new AtspmSpatSignalGroupAlignmentEvent();
            alignmentEvent.setSignalId(signalId);
            alignmentEvent.setSignalId(signalId);
            alignmentEvent.setIntersectionDescription(signal.getDescription());
            alignmentEvent.getSpatSignalGroupIds().addAll(signalGroupSet);
            if (phaseMultimap.containsKey(signalId)) {
                Collection<Integer> phases = phaseMultimap.get(signalId);
                alignmentEvent.getAtspmPhases().addAll(phases);
            }
            var setDiff = Sets.symmetricDifference(alignmentEvent.getSpatSignalGroupIds(), alignmentEvent.getAtspmPhases());
            if (!setDiff.isEmpty()) {
                result.add(alignmentEvent);
            }
        }

        return result;

    }
}
