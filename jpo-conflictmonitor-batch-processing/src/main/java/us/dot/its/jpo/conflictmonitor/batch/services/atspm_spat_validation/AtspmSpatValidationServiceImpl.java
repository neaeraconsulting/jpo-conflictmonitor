package us.dot.its.jpo.conflictmonitor.batch.services.atspm_spat_validation;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.RouteConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.SignalConfig;
import us.dot.its.jpo.conflictmonitor.batch.events.AtspmSpatSignalGroupAlignmentEvent;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.*;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPair;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.SignalGroupPhaseMap;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.SignalGroupPhases;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupIndicationLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatSignalIndication;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.TimestampedIndication;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.batch.services.spat.ProcessedSpatService;

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

    @Autowired
    public AtspmSpatValidationServiceImpl(AtspmSpatValidationParameters parameters,
            AtspmClientService atspmClientService, ProcessedSpatService spatService) {
        this.parameters = parameters;
        this.atspmClientService = atspmClientService;
        this.spatService = spatService;
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

        // Map of signal IDs to phase numbers contained in the log file
        Multimap<String, Integer> phaseMultimap = atspmLog.signalToPhaseMultimap();

        SignalIdToPhaseEventsMap signalPhaseMap = atspmLog.getSignalPhaseMap();

        // Get Spats for each intersection/signal on the route
        for (SignalConfig signalConfig : routeConfig.getSignals()) {

            final Integer intersectionId = signalConfig.getIntersectionId();
            final String signalId = signalConfig.getSignalId();

            AtspmSpatPairLog pairLog = new AtspmSpatPairLog();
            pairLog.setRouteId(routeId);

            pairLog.setSignalId(signalId);
            pairLog.setStartTime(startTime);
            pairLog.setEndTime(endTime);
            pairLog.setAtspmSpatPairs(new ArrayList<>());
            logs.add(pairLog);

            if (intersectionId == null) {
                String msg = String.format("Missing intersection id for signal %s", signalConfig);
                pairLog.setError(msg);
                log.warn(msg);
                continue;
            }

            pairLog.setIntersectionId(intersectionId);

            SignalGroupIndicationLog spatLog = spatService.signalGroupIndicationLogs(intersectionId, startTime, endTime);
            log.info("Got spatLog for signal {}", signalConfig);


            if (!signalPhaseMap.containsKey(signalId)) {
                String msg = String.format("ATSPM Signal phase map has no entries for signalId %s", signalId);
                pairLog.setError(msg);
                log.warn(msg);
                continue;
            }


            final PhaseToEventsMap phaseMap = signalPhaseMap.getPhaseMap(signalId);

            var signalGroupPhaseMap = SignalGroupPhaseMap.fromSignalConfig(signalConfig);
            SignalGroupPhases signalGroupPhases = signalGroupPhaseMap.phases();

            SignalGroupIndicationLog.SignalGroupIndicationMap signalGroupMap = spatLog.getIndicationsMap();
            Set<Integer> signalGroupSet = spatLog.getIndicationsMap().keySet();
            Set<Integer> mappedPhaseSetFromSpats = signalGroupPhases.phases(signalGroupSet);
            Set<Integer> phaseSetFromAtspm = new HashSet<>(phaseMultimap.get(signalId));
            Set<Integer> commonSignalGroupPhaseNumbers = Sets.intersection(mappedPhaseSetFromSpats, phaseSetFromAtspm);


            for (final Integer signalGroup : signalGroupMap.keySet()) {

                if (!commonSignalGroupPhaseNumbers.contains(signalGroup)) {
                    log.info("Mapped phases {} for signal group {} (of intersectionId {}, signalId {}) from the SPATs" +
                                    " don't have any matching phases in the ATSPM data," +
                                    " ignoring this signal group, see AtspmSpatSignalGroupAlignmentEvent",
                            mappedPhaseSetFromSpats, signalGroup, intersectionId, signalId);
                    continue;
                }

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
                        pair.setAtspmSecondaryPhase(event.getSecondaryPhase());
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

        Multimap<String, Integer> phaseMultimap = atspmLog.signalToPhaseMultimap();


        // Get Spats for each intersection/signal on the route
        for (SignalConfig signalConfig : routeConfig.getSignals()) {

            var signalGroupPhaseMap = SignalGroupPhaseMap.fromSignalConfig(signalConfig);
            SignalGroupPhases signalGroupPhases = signalGroupPhaseMap.phases();

            final String signalId = signalConfig.getSignalId();

            final Integer intersectionId = signalConfig.getIntersectionId();
            if (intersectionId == null) {
                String msg = String.format("Missing intersection id for signal %s", signalConfig);
                log.warn(msg);
                continue;
            }

            SignalGroupIndicationLog spatLog = spatService.signalGroupIndicationLogs(intersectionId, startTime, endTime);
            log.info("Got spatLog for signal {}", signalConfig);

            Set<Integer> signalGroupSet = spatLog.getIndicationsMap().keySet();
            Set<Integer> mappedPhaseSetFromSpats = signalGroupPhases.phases(signalGroupSet);

            var alignmentEvent = new AtspmSpatSignalGroupAlignmentEvent();
            alignmentEvent.setSignalId(signalId);
            alignmentEvent.setIntersectionID(intersectionId);
            alignmentEvent.setIntersectionDescription(signalConfig.getDescription());
            alignmentEvent.getSpatSignalGroupIds().addAll(signalGroupSet);
            alignmentEvent.getMappedPhasesFromSpats().addAll(mappedPhaseSetFromSpats);
            alignmentEvent.setStartTime(startTime);
            alignmentEvent.setEndTime(endTime);
            if (phaseMultimap.containsKey(signalId)) {
                Collection<Integer> phases = phaseMultimap.get(signalId);
                alignmentEvent.getAtspmPhases().addAll(phases);
            }
            var setDiff = Sets.symmetricDifference(alignmentEvent.getMappedPhasesFromSpats(), alignmentEvent.getAtspmPhases());

            // Check if both unmapped signal group ids, and that mapped phases for signal groups map actual atspm phases
            if (alignmentEvent.getSpatSignalGroupIds().size() != alignmentEvent.getAtspmPhases().size()
                    || !setDiff.isEmpty()) {
                result.add(alignmentEvent);
            }
        }

        result.sort(Comparator.comparing(AtspmSpatSignalGroupAlignmentEvent::getSignalId));
        return result;

    }


}
