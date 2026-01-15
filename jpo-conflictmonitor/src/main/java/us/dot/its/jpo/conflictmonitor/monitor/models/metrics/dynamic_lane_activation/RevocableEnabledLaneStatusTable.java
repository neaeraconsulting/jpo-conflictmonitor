package us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation;

import java.util.TreeMap;

/**
 * Table of the status of each revocable lane at an intersection over time.
 * <p>key = Lane ID</p>
 * <p>value = Time series of status changes</p>
 * <p>
 *     Structure:
 *     <pre>
 *         revocable laneId:
 *              timestamp   enabled/disabled
 *              timestamp   enabled/disabled
 *              ...
 *         revocable laneId:
 *              timestamp   enabled/disabled
 *              timestamp   enabled/disabled
 *              ...
 *         ...
 *     </pre>
 * </p>
 */
public class RevocableEnabledLaneStatusTable extends TreeMap<Integer, RevocableEnabledLaneStatusChanges> {
}
