package us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation;

import lombok.Data;

/**
 * A timestamped enabled/disabled status of a revocable lane
 */
@Data
public class RevocableEnabledStatus {
    private long timestamp;
    private boolean enabled;
}
