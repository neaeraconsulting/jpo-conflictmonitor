package us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
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
@JsonIgnoreProperties(ignoreUnknown = true)
public class DynamicLaneActivationMetrics extends Metrics<RsuIntersectionKey> {
    public DynamicLaneActivationMetrics() {
        super("DynamicLaneActivation");
    }

    /**
     * The SPAT source (usually the IP address of the RSU)
     */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private String getSource() {
        return key != null ? key.getRsuId() : null;
    }

    /**
     * The intersection ID
     */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private int getIntersectionID() {
        return key != null ? key.getIntersectionId() : -1;
    }

    /**
     * The RoadRegulatorID (region)
     */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private int getRoadRegulatorID() {
        return key != null ? key.getRegion() : -1;
    }

    /**
     * Table of the enabled status of each revocable lane in the intersection of the time period
     * of the metric.
     */
    private RevocableEnabledLaneStatusTable revocableEnabledLaneStatusTable = new RevocableEnabledLaneStatusTable();

}


