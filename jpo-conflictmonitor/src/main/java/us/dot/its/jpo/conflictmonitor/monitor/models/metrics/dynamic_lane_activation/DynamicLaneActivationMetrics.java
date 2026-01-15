package us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.revocable_enabled_lane_alignment.LaneTypeAttributesMap;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.Metrics;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;

/**
 * Performance and Operational Metrics: Dynamic Lane Activation Monitoring
 * Object Tracks which revocable lanes are active per rsu/intersection (for intersections
 * that have them).
 * <p>Notes:</p>
 * <ul>
 *     <li>The time period for the metric corresponds to timestamps of SPATs that were received for which the set of
 *     enabled revocable lanes changed.</li>
 *      <li>This metric is only produced when the enabled status of any revocable lane in the intersection changes.</li>
 *      <li>This metric is <b>not</b> produced for every SPAT message.</li>
 *      <li>This metric is <b>not</b> produced for intersections that don't have any revocable lanes (and most
 *      intersections generally don't have any)</li>
 * </ul>
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class DynamicLaneActivationMetrics extends Metrics<RsuIntersectionKey> {
    public DynamicLaneActivationMetrics() {
        super("DynamicLaneActivation");
    }

    /**
     * The SPAT source (usually the IP address of the RSU)
     */
    private String source;

    /**
     * The intersection ID
     */
    private int intersectionID;

    /**
     * The RoadRegulatorID (region)
     */
    private int roadRegulatorID;

    /**
     * Map of LaneID to DE_LaneTypeAttributes, including all lanes, revocable or not
     */
    private LaneTypeAttributesMap laneTypeAttributes;

    /**
     * Table of the enabled status of each revocable lane in the intersection of the time period
     * of the metric.
     */
    private RevocableEnabledLaneStatusTable revocableEnabledLaneStatusMap;

}


