package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.*;

/**
 * Signal Group Delta Series for an intersection
 */
@Document("CmAtspmSpatSignalGroupStateLog")
@Data
public class SignalGroupStateLog {
    private int intersectionId;
    private Instant startTime;
    private Instant endTime;
    private Map<Integer, List<TimestampedState>> signalGroupStates;
    public static SignalGroupStateLog fromSpatLog(SpatLog spatLog) {
        var log = new SignalGroupStateLog();
        log.setIntersectionId(spatLog.getIntersectionId());
        log.setStartTime(spatLog.getStartTime());
        log.setEndTime(spatLog.getEndTime());

        ListMultimap<Integer, SignalGroupState> sigMultimap = ArrayListMultimap.create();

        // Organize by signal group
        for (Spat spat : spatLog.getSpats()) {
            for (SignalGroupState state : spat.getStates()) {
                sigMultimap.put(state.getSignalGroup(), state);
            }
        }

        log.signalGroupStates = new TreeMap<>();

        // Remove unchanged signal group states from each signal group,
        // keep only first changes signal state
        for (int signalGroup : sigMultimap.keySet()) {
            List<SignalGroupState> states = sigMultimap.get(signalGroup);
            List<TimestampedState> stateDeltas = new ArrayList<>();
            SignalGroupState previousState = null;
            for (SignalGroupState state : states) {
                if (previousState == null || state.getEventState() != previousState.getEventState()) {
                    stateDeltas.add(TimestampedState.fromSignalGroupState(state));
                }
                previousState = state;
            }
            log.signalGroupStates.put(signalGroup, stateDeltas);
        }
        return log;
    }
}
