package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TimestampedIndication {
    private Instant timestamp;
    private SpatSignalIndication indication;
}
