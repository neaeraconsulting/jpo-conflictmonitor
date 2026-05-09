package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

@Slf4j
public class PhaseToEventsMap extends TreeMap<Integer, List<ProcessedControllerEvent>> {
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

        // A binary search would be better, but this works fine
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

        if (nearest == null) {
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

    public record FindEventInWindowResult(boolean paired, ProcessedControllerEvent event) {}
}

