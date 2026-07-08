package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.MetricsAlgorithmInterface;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.revocable_enabled_lane_alignment.RevocableEnabledLaneAlignmentAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation.DynamicLaneActivationMetrics;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;

/**
 * Dynamic Lane Activation Metrics Algorithm
 * <p>Tracks revocable lane enabled status</p>
 * <p>Plugs into the {@link RevocableEnabledLaneAlignmentAlgorithm} as a subtopology</p>
 */
public interface DynamicLaneActivationMetricsAlgorithm
    extends MetricsAlgorithmInterface<
        DynamicLaneActivationMetrics,
        RsuIntersectionKey,
        DynamicLaneActivationMetricsParameters> {
}
