package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.mapping.Document;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.RouteConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.SignalConfig;
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
    private final RouteConfig routeConfig;

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
            Clock clock, ZoneId localTimeZone, RouteConfig routeConfig) {
        this.routeId = routeId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.routeConfig = routeConfig;

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
                // Sort by timestmap
                eventList.sort(Comparator.comparing(ProcessedControllerEvent::getTimestamp));
            }
        }

        // Merge secondary with primary phases
        for (String signalId : signalPhaseMap.keySet()) {
            SignalConfig signalConfig = routeConfig.signalConfig(signalId);
            SignalGroupPhaseMap signalGroupPhaseMap = SignalGroupPhaseMap.fromSignalConfig(signalConfig);
            PhaseToEventsMap phaseMap = signalPhaseMap.getPhaseMap(signalId);
            for (final int phase : phaseMap.keySet()) {
                // primary phases for which this is secondary
                Set<Integer> primaryPhases = signalGroupPhaseMap.primaryPhasesForSecondary(phase);
                log.info("Primary phases are {} for secondary phase {}", primaryPhases, phase);
                for (final int primaryPhase : primaryPhases) {
                    List<ProcessedControllerEvent> primaryEventList = phaseMap.getEventList(primaryPhase);
                    List<ProcessedControllerEvent> secondaryEventList = phaseMap.getEventList(phase);
                    List<ProcessedControllerEvent> mergedList = merge(primaryEventList, secondaryEventList);
                    phaseMap.replace(primaryPhase, mergedList);
                }
            }
        }



    }

    // Assumes both lists are sorted
    private List<ProcessedControllerEvent> merge(
            List<ProcessedControllerEvent> primaryEventList,
            List<ProcessedControllerEvent> secondaryEventList) {

        List<MergedEvent> mergedList = new ArrayList<>();
        for (ProcessedControllerEvent event : primaryEventList) {
            mergedList.add(new MergedEvent(true, event));
        }
        for (ProcessedControllerEvent event : secondaryEventList) {
            mergedList.add(new MergedEvent(false, event));
        }
        mergedList.sort((me1, me2) -> {
            int compareTimestamps = me1.event().getTimestamp().compareTo(me2.event().getTimestamp());
            if (compareTimestamps != 0) return compareTimestamps;
            // if timestamps are the same, sort by secondary first, then primary
            return -Boolean.compare(me1.isPrimary, me2.isPrimary);
        });

        List<ProcessedControllerEvent> resultant = new ArrayList<>();

        ProcessedControllerEvent mostRecentSecondaryEvent = null;

        boolean primaryIsRed = false;
        ProcessedControllerEvent redPrimaryEvent = null;

        for (MergedEvent mergedEvent : mergedList) {
            boolean isPrimary = mergedEvent.isPrimary();
            var event = mergedEvent.event();
            var eventCode = event.getEventCode();
            if (isPrimary) {
                // Primary phase event
                primaryIsRed = (eventCode == EventCode.RED);
                if (primaryIsRed) {
                    redPrimaryEvent = event;
                    // Primary is red: use the most recent secondary (if any), timestamped at
                    // the moment the primary itself went red - not the (possibly much older)
                    // timestamp of that secondary transition - since this resultant entry
                    // represents the inferred indication as of *now*, not a re-emission of
                    // that earlier secondary transition.
                    if (mostRecentSecondaryEvent != null) {
                        resultant.add(mergeEvents(event, mostRecentSecondaryEvent, event.getTimestamp()));
                    } else {
                        resultant.add(event);
                    }
                } else {
                    // Primary is not red: always use it
                    redPrimaryEvent = null;
                    resultant.add(event);
                }
            } else {
                // Secondary phase event
                mostRecentSecondaryEvent = event;
                // This might be an update to the most recent secondary during a primary red phase:
                // if so, use it, timestamped at this (current) secondary transition.
                if (primaryIsRed) {
                    resultant.add(mergeEvents(redPrimaryEvent, mostRecentSecondaryEvent, mostRecentSecondaryEvent.getTimestamp()));
                }
            }
        }

        return resultant;
    }

    // Merge secondary with primary with secondary dominant (for when primary is red).
    // The caller supplies the timestamp explicitly, since it must be the timestamp of
    // whichever transition (primary or secondary) is actually driving this resultant entry -
    // it is not always the secondary event's own timestamp.
    private ProcessedControllerEvent mergeEvents(
            ProcessedControllerEvent primaryEvent, ProcessedControllerEvent secondaryEvent, Instant timestamp) {
        var event = new ProcessedControllerEvent();
        event.setEventCode(secondaryEvent.getEventCode());
        event.setTimestamp(timestamp);
        event.setSignalId(secondaryEvent.getSignalId());
        event.setSecondaryPhase(secondaryEvent.getPhase());
        event.setPhase(primaryEvent.getPhase());
        return event;
    }

    public record MergedEvent(boolean isPrimary, ProcessedControllerEvent event) {}

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
