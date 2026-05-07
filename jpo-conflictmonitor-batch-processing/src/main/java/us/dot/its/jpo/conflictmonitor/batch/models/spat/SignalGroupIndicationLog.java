package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.*;

@Document("CmAtspmSpatSignalGroupIndicationLog")
@Data
public class SignalGroupIndicationLog {
    private int intersectionId;
    private Instant startTime;
    private Instant endTime;
    private SignalGroupIndicationMap indicationsMap;
    public static SignalGroupIndicationLog fromSignalGroupStateLog(SignalGroupStateLog stateLog) {
        var log = new SignalGroupIndicationLog();
        log.setIntersectionId(stateLog.getIntersectionId());
        log.setStartTime(stateLog.getStartTime());
        log.setEndTime(stateLog.getEndTime());
        log.indicationsMap = new SignalGroupIndicationMap();

        // Convert state to indication
        Map<Integer, List<TimestampedState>> stateMap = stateLog.getSignalGroupStates();
        for (int signalGroup : stateMap.keySet()) {
            List<TimestampedState> states = stateMap.get(signalGroup);
            for (TimestampedState state : states) {
                var tsIndication = new TimestampedIndication();
                var indication = SpatSignalIndication.fromMovementPhaseState(state.getEventState());
                if (indication.isPresent()) {
                    tsIndication.setTimestamp(state.getTimestamp());
                    tsIndication.setIndication(indication.get());
                    tsIndication.setMovementPhaseState(state.getEventState());
                    log.indicationsMap.putIndication(signalGroup, tsIndication);
                }
            }
        }

        // Remove unchanged indications
        for (int signalGroup : log.indicationsMap.keySet()) {
            List<TimestampedIndication> indications = log.indicationsMap.get(signalGroup);
            List<TimestampedIndication> indicationDeltas = new ArrayList<>();
            TimestampedIndication previousIndication = null;
            for (TimestampedIndication indication : indications) {
                if (previousIndication == null || indication.getIndication() != previousIndication.getIndication()) {
                    indicationDeltas.add(indication);
                }
                previousIndication = indication;
            }
            log.indicationsMap.replace(signalGroup, indicationDeltas);
        }
        return log;
    }

    public static class SignalGroupIndicationMap extends TreeMap<Integer, List<TimestampedIndication>> {
        public List<TimestampedIndication> getIndications(int signalGroup) {
            if (containsKey(signalGroup)) {
                return this.get(signalGroup);
            }
            return new ArrayList<>();
        }
        public void putIndication(int signalGroup, TimestampedIndication indication) {
            if (this.containsKey(signalGroup)) {
                get(signalGroup).add(indication);
            } else {
                List<TimestampedIndication> list = new ArrayList<>();
                list.add(indication);
                this.put(signalGroup, list);
            }
        }
    }
}
