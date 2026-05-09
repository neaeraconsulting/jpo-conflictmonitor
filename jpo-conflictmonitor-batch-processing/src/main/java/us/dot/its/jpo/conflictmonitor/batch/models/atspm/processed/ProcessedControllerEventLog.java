package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.mapping.Document;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.RouteConfig;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.ControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.SignalGroupPhaseMap;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;

@Slf4j
@Data
@Document(collection = "CmAtspmProcessedControllerEventLog")
public class ProcessedControllerEventLog {

    private int routeId;
    private Instant startTime;
    private Instant endTime;

    @JsonIgnore
    private final SignalGroupPhaseMap signalGroupPhaseMap;

    private SignalIdToPhaseEventsMap signalPhaseMap;

    public long size() {
        if (signalPhaseMap == null) return 0;
        long count = 0;
        for (Map<Integer, List<ProcessedControllerEvent>> phaseMap : signalPhaseMap.values()) {
            for (List<ProcessedControllerEvent> eventList : phaseMap.values()) {
                count += eventList.size();
            }
        }
        return count;
    }

    public ProcessedControllerEventLog(
            int routeId, Instant startTime, Instant endTime, Collection<ControllerEventLog> controllerEvent,
            Clock clock, ZoneId localTimeZone, SignalGroupPhaseMap signalGroupPhaseMap) {
        this.routeId = routeId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.signalGroupPhaseMap = signalGroupPhaseMap;

        // Group by signal ID and Phase
        signalPhaseMap = new SignalIdToPhaseEventsMap();
        for (ControllerEventLog event : controllerEvent) {
            Optional<ProcessedControllerEvent> pceOpt = ProcessedControllerEvent.fromControllerEventLog(event, clock, localTimeZone);
            pceOpt.ifPresent(e -> signalPhaseMap.putEvent(e.getSignalId(), e));
        }

        // Sort
        for (String signalId : signalPhaseMap.keySet()) {
            PhaseToEventsMap phaseMap = signalPhaseMap.getPhaseMap(signalId);
            for (Integer phase : phaseMap.keySet()) {
                List<ProcessedControllerEvent> eventList = phaseMap.get(phase);
                eventList.sort(Comparator.comparingInt(ProcessedControllerEvent::getPhase));
            }
        }

        // Merge secondary with primary phases
        for (String signalId : signalPhaseMap.keySet()) {
            PhaseToEventsMap phaseMap = signalPhaseMap.getPhaseMap(signalId);
            for (final int phase : phaseMap.keySet()) {
                // primary phases for which this is secondary
                Set<Integer> primaryPhases = signalGroupPhaseMap.primaryPhasesForSecondary(phase);
                for (final int primaryPhase : primaryPhases) {
                    List<ProcessedControllerEvent> primaryEventList = phaseMap.getEventList(primaryPhase);
                    List<ProcessedControllerEvent> secondaryEventList = phaseMap.getEventList(phase);
                    List<ProcessedControllerEvent> mergedList = merge(primaryEventList, secondaryEventList);
                    phaseMap.replace(primaryPhase, mergedList);
                }
            }
        }



    }

    private List<ProcessedControllerEvent> merge(List<ProcessedControllerEvent> primaryEventList, List<ProcessedControllerEvent> secondaryEventList) {
        return primaryEventList;
    }

    @JsonIgnore
    public Multimap<String, Integer> signalToPhaseMultimap() {
        SignalIdToPhaseEventsMap signalPhaseMap = getSignalPhaseMap();
        Multimap<String, Integer> phaseMultimap = MultimapBuilder.hashKeys().arrayListValues().build();
        for (String signalId : signalPhaseMap.keySet()) {
            PhaseToEventsMap phaseMap = signalPhaseMap.getPhaseMap(signalId);
            for (final Integer phase : phaseMap.keySet()) {
                List<ProcessedControllerEvent> eventList = phaseMap.getEventList(phase);
                for (final ProcessedControllerEvent event : eventList) {
                    phaseMultimap.put(signalId, event.getPhase());
                }
            }
        }
        return phaseMultimap;
    }

}
