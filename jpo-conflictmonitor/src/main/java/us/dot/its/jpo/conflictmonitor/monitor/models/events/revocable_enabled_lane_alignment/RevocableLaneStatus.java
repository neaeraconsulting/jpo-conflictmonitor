package us.dot.its.jpo.conflictmonitor.monitor.models.events.revocable_enabled_lane_alignment;

import lombok.Data;
import lombok.Generated;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.Set;

/**
 * Lists all the revocable lanes for an intersection and their enabled/disabled status
 * for a particular SPAT timestamp.
 * This type is produced for all revocable enabled lane status changes and used for the
 * Dynamic Lane Activation Metric.
 */
@Data
@NoArgsConstructor
@Generated
public class RevocableLaneStatus {

    public RevocableLaneStatus(RevocableEnabledLaneAlignmentEvent event) {
        if (event != null) {
            this.source = event.getSource();
            this.intersectionID = event.getIntersectionID();
            this.roadRegulatorID = event.getRoadRegulatorID();
            this.timestamp = event.getTimestamp();
            this.revocableLaneList = event.getRevocableLaneList();
            this.enabledLaneList = event.getEnabledLaneList();
        }
    }

    /**
     * The SPAT source (usually IP address)
     */
    private String source;

    /**
     * The Intersection ID
     */
    private int intersectionID = -1;

    /**
     * The Road Regulator ID
     */
    private int roadRegulatorID = -1;

    /**
     * The timestamp of the SPAT
     */
    private long timestamp;

    /**
     * Set of LaneIDs with the 'revocable' bit set in the MAP message.
     */
    private Set<Integer> revocableLaneList = new HashSet<>();

    /**
     * Set of enabled Lane IDs from the SPAT message.
     */
    private Set<Integer> enabledLaneList = new HashSet<>();

    /**
     * Flag to indicate it this is a "tick" in the processing topology, not an actual status.
     * A tick is an artificial event created by the topology to ensure the time window for the metrics
     * closes in the absence of real events, because real events may be sparse.
     */
    private boolean tick = false;


}
