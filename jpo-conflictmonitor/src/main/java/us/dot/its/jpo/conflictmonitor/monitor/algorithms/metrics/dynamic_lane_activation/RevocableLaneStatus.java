package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation;

import lombok.Data;
import lombok.Generated;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.revocable_enabled_lane_alignment.LaneTypeAttributesMap;

import java.util.Set;

/**
 * Lists all the revocable lanes for an intersection and their enabled/disabled status
 * for a particular SPAT timestamp.
 */
@Data
@Generated
public class RevocableLaneStatus {
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
     * The lane ID within the intersection
     */
    private int laneID;

    /**
     * The timestamp of the SPAT
     */
    private long timestamp;

    /**
     * Map of LaneID to DE_LaneTypeAttributes, including all lanes, revocable or not
     */
    private LaneTypeAttributesMap laneTypeAttributes;

    /**
     * Set of LaneIDs with the 'revocable' bit set in the MAP message.
     */
    private Set<Integer> revocableLaneList;

    /**
     * Set of enabled Lane IDs from the SPAT message.
     */
    private Set<Integer> enabledLaneList;
}
