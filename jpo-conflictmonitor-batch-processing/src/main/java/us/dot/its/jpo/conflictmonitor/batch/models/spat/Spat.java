package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import lombok.Data;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;


@Data
public class Spat {
    private Instant timestamp;
    private List<SignalGroupState> states;
    public static Spat fromProcessedSpat(ProcessedSpat spat) {
        final Spat rs = new Spat();
        if (spat.getUtcTimeStamp() != null) {
            rs.timestamp = spat.getUtcTimeStamp().toInstant();
        }
        rs.states = spat.getStates().stream()
                .map(state -> SignalGroupState.fromMovementState(state, rs.timestamp))
                .sorted(Comparator.comparing(SignalGroupState::getSignalGroup))
                .toList();
        return rs;
    }
}
