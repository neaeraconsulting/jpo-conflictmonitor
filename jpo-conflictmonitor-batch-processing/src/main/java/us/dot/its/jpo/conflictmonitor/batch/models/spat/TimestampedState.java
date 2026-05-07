package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TimestampedState {
    private Instant timestamp;
    private ProcessedMovementPhaseState eventState;
    public static TimestampedState fromSignalGroupState(SignalGroupState state) {
        return new TimestampedState(state.getTimestamp(), state.getEventState());
    }
}
