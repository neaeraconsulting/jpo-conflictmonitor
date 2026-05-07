package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import lombok.Data;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
public class SignalGroupIndicationLog {
    private int intersectionId;
    private Instant startTime;
    private Instant endTime;
    private Map<Integer, List<TimestampedIndication>> signalGroupIndications;
    public static SignalGroupIndicationLog fromSignalGroupStateLog(SignalGroupStateLog stateLog) {
        var log = new SignalGroupIndicationLog();
        log.setIntersectionId(stateLog.getIntersectionId());
        log.setStartTime(stateLog.getStartTime());
        log.setEndTime(stateLog.getEndTime());

        // Convert state to indication
        Map<Integer, List<TimestampedState>> stateMap = stateLog.getSignalGroupStates();
        ListMultimap<Integer, TimestampedIndication> indicationMultimap = ArrayListMultimap.create();
        for (int signalGroup : stateMap.keySet()) {
            List<TimestampedState> states = stateMap.get(signalGroup);
            for (TimestampedState state : states) {
                var tsIndication = new TimestampedIndication();
                var indication = SpatSignalIndication.fromMovementPhaseState(state.getEventState());
                if (indication.isPresent()) {
                    tsIndication.setTimestamp(state.getTimestamp());
                    tsIndication.setIndication(indication.get());
                    indicationMultimap.put(signalGroup, tsIndication);
                }
            }
        }

        log.signalGroupIndications = new LinkedHashMap<>();

        // Remove unchanged indications
        for (int signalGroup : indicationMultimap.keySet()) {
            List<TimestampedIndication> indications = indicationMultimap.get(signalGroup);
            List<TimestampedIndication> indicationDeltas = new ArrayList<>();
            TimestampedIndication previousIndication = null;
            for (TimestampedIndication indication : indications) {
                if (previousIndication == null || indication.getIndication() != previousIndication.getIndication()) {
                    indicationDeltas.add(indication);
                }
                previousIndication = indication;
            }
            log.signalGroupIndications.put(signalGroup, indicationDeltas);
        }
        return log;
    }
}
