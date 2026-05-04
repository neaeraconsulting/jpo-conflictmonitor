package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import com.google.common.collect.*;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.ControllerEventLog;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

@Data
@Document(collection = "CmAtspmProcessedControllerEventLog")
public class ProcessedControllerEventLog {

    private int routeId;
    private Instant startTime;
    private Instant endTime;

    private Map<String, Map<Integer, List<ProcessedControllerEvent>>> events;

    public long size() {
        if (events == null) return 0;
        long count = 0;
        for (Map<Integer, List<ProcessedControllerEvent>> phaseMap : events.values()) {
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

        // Group by signal ID
        var multimap = ArrayListMultimap.<String, ProcessedControllerEvent>create();
        for (ControllerEventLog event : controllerEvent) {
            Optional<ProcessedControllerEvent> pceOpt = ProcessedControllerEvent.fromControllerEventLog(event, clock, localTimeZone);
            pceOpt.ifPresent(e -> multimap.put(e.getSignalId(), e));
        }

        // Group by phase
        var signalPhaseMultimap = new LinkedHashMap<String, ListMultimap<Integer, ProcessedControllerEvent>>();
        for (String signalId : multimap.keySet()) {
            List<ProcessedControllerEvent> eventList = multimap.get(signalId);
            var phaseMultimap = ArrayListMultimap.<Integer, ProcessedControllerEvent>create();
            for (ProcessedControllerEvent event : eventList) {
                phaseMultimap.put(event.getPhase(), event);
            }
            signalPhaseMultimap.put(signalId, phaseMultimap);
        }

        // Convert to normal Map structure that Mongo can deal with
        events = new LinkedHashMap<>();
        for (String signalId : signalPhaseMultimap.keySet()) {
            ListMultimap<Integer, ProcessedControllerEvent> phaseMultimap = signalPhaseMultimap.get(signalId);
            Map<Integer, List<ProcessedControllerEvent>> phaseMap = new LinkedHashMap<>();
            for (Integer phase : phaseMultimap.keySet()) {
                List<ProcessedControllerEvent> eventList = phaseMultimap.get(phase);
                eventList.sort(Comparator.comparingInt(ProcessedControllerEvent::getPhase));
                phaseMap.put(phase, eventList);
            }
            events.put(signalId, phaseMap);
        }

    }





}
