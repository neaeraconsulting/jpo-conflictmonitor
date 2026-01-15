package us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation;

import java.util.TreeMap;

/**
 * A time series of enabled/disabled status changes for a revocable one Lane.
 * <p>key = Timestamp (epoch millisecond) of the SPAT when the revocable lane status changed</p>
 * <p>value = The status of the revocable lane after the change (true = enabled, false = disabled)</p>
 */
public class RevocableEnabledLaneStatusChanges extends TreeMap<Long, Boolean> {
}
