package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TimestampedIndication {
    private Instant timestamp;
    private SpatSignalIndication indication;
    private ProcessedMovementPhaseState movementPhaseState;
}
