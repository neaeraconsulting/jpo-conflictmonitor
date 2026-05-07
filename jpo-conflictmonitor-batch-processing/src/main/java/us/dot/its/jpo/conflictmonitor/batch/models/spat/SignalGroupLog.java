package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import lombok.Data;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Signal Group Delta Series for an intersection
 */
@Data
public class SignalGroupLog {
    private int intersectionId;
    private Instant startTime;
    private Instant endTime;
    private Map<Integer, List<SignalGroupState>> stateMap;
    public static SignalGroupLog fromSpatLog(SpatLog spatLog) {
        SignalGroupLog signalGroupLog = new SignalGroupLog();
        signalGroupLog.setIntersectionId(spatLog.getIntersectionId());
        signalGroupLog.setStartTime(spatLog.getStartTime());
        signalGroupLog.setEndTime(spatLog.getEndTime());

        ListMultimap<Integer, SignalGroupState> sigMultimap = ArrayListMultimap.create();

        // Organize by signal group
        for (Spat spat : spatLog.getSpats()) {
            for (SignalGroupState state : spat.getStates()) {
                sigMultimap.put(state.getSignalGroup(), state);
            }
        }

        signalGroupLog.stateMap = new LinkedHashMap<>();

        // Remove unchanged signal group states from each signal group,
        // keep only first changes signal state
        for (int signalGroup : sigMultimap.keySet()) {
            List<SignalGroupState> states = sigMultimap.get(signalGroup);
            List<SignalGroupState> stateDeltas = new ArrayList<>();
            SignalGroupState previousState = null;
            for (SignalGroupState state : states) {
                if (previousState == null || state.getEventState() != previousState.getEventState()) {
                    stateDeltas.add(state);
                }
                previousState = state;
            }
            signalGroupLog.stateMap.put(signalGroup, stateDeltas);
        }
        return signalGroupLog;
    }
}
