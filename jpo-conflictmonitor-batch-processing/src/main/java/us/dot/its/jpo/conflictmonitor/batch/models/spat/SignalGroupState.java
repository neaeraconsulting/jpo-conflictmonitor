package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import lombok.Data;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementEvent;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementState;

import java.time.Instant;

@Data
public class SignalGroupState {
    private int signalGroup;
    private Instant timestamp;
    private ProcessedMovementPhaseState eventState;
    public static SignalGroupState fromMovementState(ProcessedMovementState state, Instant timestamp) {
        var signalGroupState = new SignalGroupState();
        signalGroupState.setSignalGroup(state.getSignalGroup());
        signalGroupState.setTimestamp(timestamp);
        ProcessedMovementEvent event = state.getStateTimeSpeed().getFirst();
        signalGroupState.setEventState(event.getEventState());
        return signalGroupState;
    }
}
