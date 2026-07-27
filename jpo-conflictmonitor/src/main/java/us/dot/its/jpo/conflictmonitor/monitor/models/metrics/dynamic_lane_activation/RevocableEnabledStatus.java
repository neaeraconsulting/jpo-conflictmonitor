package us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation;

import lombok.Data;

/**
 * A timestamped enabled/disabled status of a revocable lane
 */
public record RevocableEnabledStatus (
    long timestamp,
    boolean enabled
){}
