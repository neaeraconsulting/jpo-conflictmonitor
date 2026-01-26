package us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation;

import lombok.Data;

/**
 * A time series of enabled/disabled status changes for one revocable one Lane.
 */
@Data
public class RevocableEnabledLaneStatusChanges {
    /**
     * The lane ID
     */
    private int laneID;

    /**
     * List of enabled status changes for the revocable lane
     */
    private RevocableEnabledStatusList statusChanges = new RevocableEnabledStatusList();
}
