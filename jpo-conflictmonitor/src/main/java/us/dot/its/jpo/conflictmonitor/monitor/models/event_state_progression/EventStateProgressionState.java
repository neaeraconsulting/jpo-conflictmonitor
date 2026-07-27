package us.dot.its.jpo.conflictmonitor.monitor.models.event_state_progression;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;

/**
 * Slim phase snapshot for Event State Progression RocksDB stores.
 * Avoids persisting timing-heavy {@link SpatMovementState} JSON.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventStateProgressionState {

    private int signalGroup;
    private ProcessedMovementPhaseState phaseState;
    private long utcTimeStamp;

    public static EventStateProgressionState from(SpatMovementState state) {
        return new EventStateProgressionState(
                state.getSignalGroup(),
                state.getPhaseState(),
                state.getUtcTimeStamp());
    }

    public SpatMovementState toSpatMovementState() {
        SpatMovementState state = new SpatMovementState();
        state.setSignalGroup(signalGroup);
        state.setPhaseState(phaseState);
        state.setUtcTimeStamp(utcTimeStamp);
        return state;
    }
}
