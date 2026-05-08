package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.mapping.Document;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.ControllerEventLog;

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

    private SignalPhaseMap signalPhaseMap;

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
            Clock clock, ZoneId localTimeZone) {
        this.routeId = routeId;
        this.startTime = startTime;
        this.endTime = endTime;

        // Group by signal ID and Phase
        signalPhaseMap = new SignalPhaseMap();
        for (ControllerEventLog event : controllerEvent) {
            Optional<ProcessedControllerEvent> pceOpt = ProcessedControllerEvent.fromControllerEventLog(event, clock, localTimeZone);
            pceOpt.ifPresent(e -> signalPhaseMap.putEvent(e.getSignalId(), e));
        }

        // Sort
        for (String signalId : signalPhaseMap.keySet()) {
            PhaseMap phaseMap = signalPhaseMap.getPhaseMap(signalId);
            for (Integer phase : phaseMap.keySet()) {
                List<ProcessedControllerEvent> eventList = phaseMap.get(phase);
                eventList.sort(Comparator.comparingInt(ProcessedControllerEvent::getPhase));
            }
        }

    }


    public static class SignalPhaseMap extends TreeMap<String, PhaseMap> {
        public PhaseMap getPhaseMap(String signalId) {
            if (!containsKey(signalId)) return new PhaseMap();
            return get(signalId);
        }
        public void putPhaseMap(String signalId, PhaseMap phaseMap) {
            put(signalId, phaseMap);
        }
        public void putEvent(String signalId, ProcessedControllerEvent event) {
            if (containsKey(signalId)) {
                getPhaseMap(signalId).putEvent(event.getPhase(), event);
            } else {
                PhaseMap phaseMap = new PhaseMap();
                phaseMap.putEvent(event.getPhase(), event);
                putPhaseMap(signalId, phaseMap);
            }
        }
    }

    public static class PhaseMap extends TreeMap<Integer, List<ProcessedControllerEvent>> {
        public List<ProcessedControllerEvent> getEventList(int phase) {
            if (!containsKey(phase)) return new ArrayList<>();
            return get(phase);
        }
        public void putEventList(int phase, List<ProcessedControllerEvent> eventList) {
            put(phase, eventList);
        }
        public void putEvent(int phase, ProcessedControllerEvent event) {
            if (containsKey(phase)) {
                get(phase).add(event);
            } else {
                List<ProcessedControllerEvent> eventList = new ArrayList<>();
                eventList.add(event);
                put(phase, eventList);
            }
        }

        public FindEventInWindowResult findEventInWindow(final int phase, final EventCode eventCode,
                                                                    final Instant timestamp, final Duration window) {
            if (!containsKey(phase)) return new FindEventInWindowResult(false, null);
            List<ProcessedControllerEvent> eventList = getEventList(phase);
//            Optional<ProcessedControllerEvent> nearest = eventList.stream()
//                    .filter(event -> event.getEventCode() == eventCode)
//                    .min(Comparator.comparing(ProcessedControllerEvent::getTimestamp));
            long maxDiff = Long.MAX_VALUE;
            ProcessedControllerEvent nearest = null;
            for (var event : eventList) {
                if (event.getEventCode() != eventCode) continue;
                long diff = Math.abs(event.getTimestamp().getEpochSecond() - timestamp.getEpochSecond());
                if (diff < maxDiff) {
                    maxDiff = diff;
                    nearest = event;
                }
            }

            if (nearest != null) {
                log.warn("No nearest event found for phase {} with event code {}", phase, eventCode);
                return new FindEventInWindowResult(false, null);
            }
            ProcessedControllerEvent event = nearest;
            Duration diff = Duration.between(timestamp, event.getTimestamp()).abs();
            if (diff.compareTo(window) <= 0) {
                // In window
                return new FindEventInWindowResult(true, event);
            } else {
                // Not in window
                return new FindEventInWindowResult(false, event);
            }
        }
    }

    public record FindEventInWindowResult(boolean paired, ProcessedControllerEvent event) {}

    @JsonIgnore
    public Multimap<String, Integer> signalToPhaseMultimap() {
        ProcessedControllerEventLog.SignalPhaseMap signalPhaseMap = getSignalPhaseMap();
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
        return phaseMultimap;
    }

}
