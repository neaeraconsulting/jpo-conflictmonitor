package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.ConfigurableAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.revocable_enabled_lane_alignment.RevocableEnabledLaneAlignmentAlgorithm;

/**
 * Dynamic Lane Activation Metrics Algorithm
 * <p>Tracks revocable lane enabled status</p>
 * <p>Plugs into the {@link RevocableEnabledLaneAlignmentAlgorithm} as a subtopology</p>
 */
public interface DynamicLaneActivationMetricsAlgorithm
    extends ConfigurableAlgorithm<DynamicLaneActivationMetricsParameters> {
}
