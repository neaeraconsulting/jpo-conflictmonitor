package us.dot.its.jpo.conflictmonitor.monitor.models.spat;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import us.dot.its.jpo.conflictmonitor.monitor.utils.DateTimeUtils;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementEvent;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;
import us.dot.its.jpo.geojsonconverter.pojos.spat.TimingChangeDetails;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;

/**
 * Slim SPaT snapshot for message-count progression state stores.
 * Avoids persisting the full {@link ProcessedSpat} JSON in RocksDB.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SpatMessageCountState {

    private ZonedDateTime utcTimeStamp;
    private int revision;
    private int contentHash;
    private String originIp;
    private Integer intersectionId;
    private Integer region;

    public static SpatMessageCountState fromProcessedSpat(ProcessedSpat spat) {
        SpatMessageCountState state = new SpatMessageCountState();
        state.setUtcTimeStamp(spat.getUtcTimeStamp());
        state.setRevision(spat.getRevision() != null ? spat.getRevision() : 0);
        state.setContentHash(contentHash(spat));
        state.setOriginIp(spat.getOriginIp());
        state.setIntersectionId(spat.getIntersectionId());
        state.setRegion(spat.getRegion());
        return state;
    }

    /**
     * Content fingerprint ignoring utcTimeStamp, revision, and odeReceivedAt.
     * Hashes signal-group phase/timing and enabled lanes without mutating the spat
     * (avoids deep {@code spat.hashCode()} on the full object graph).
     */
    public static int contentHash(ProcessedSpat spat) {
        if (spat == null) {
            return 0;
        }
        int hash = 1;
        List<ProcessedMovementState> states = spat.getStates();
        if (states != null) {
            for (ProcessedMovementState state : states) {
                if (state == null) {
                    continue;
                }
                hash = 31 * hash + Objects.hash(state.getSignalGroup());
                List<ProcessedMovementEvent> events = state.getStateTimeSpeed();
                ProcessedMovementEvent first =
                        events != null && !events.isEmpty() ? events.getFirst() : null;
                if (first != null) {
                    hash = 31 * hash + Objects.hash(first.getEventState());
                    TimingChangeDetails timing = first.getTiming();
                    if (timing != null) {
                        hash = 31 * hash + Objects.hash(
                                DateTimeUtils.toMillis(timing.getStartTime()),
                                DateTimeUtils.toMillis(timing.getMinEndTime()),
                                DateTimeUtils.toMillis(timing.getMaxEndTime()));
                    }
                }
            }
        }
        if (spat.getEnabledLanes() != null) {
            hash = 31 * hash + spat.getEnabledLanes().hashCode();
        }
        if (spat.getStatus() != null) {
            hash = 31 * hash + spat.getStatus().hashCode();
        }
        hash = 31 * hash + Objects.hash(spat.getOriginIp(), spat.getIntersectionId(), spat.getRegion());
        return hash;
    }

    public boolean sameRevisionAndContent(SpatMessageCountState other) {
        return other != null
                && this.revision == other.revision
                && this.contentHash == other.contentHash;
    }
}
