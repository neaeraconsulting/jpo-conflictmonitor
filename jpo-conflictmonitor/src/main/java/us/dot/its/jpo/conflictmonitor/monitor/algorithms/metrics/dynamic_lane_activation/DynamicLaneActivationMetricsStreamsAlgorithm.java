package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.MetricsStreamsAlgorithmInterface;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation.DynamicLaneActivationMetrics;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;

public interface DynamicLaneActivationMetricsStreamsAlgorithm
    extends DynamicLaneActivationMetricsAlgorithm,
        MetricsStreamsAlgorithmInterface<
                RsuIntersectionKey,
                RsuIntersectionKey,
                RevocableLaneStatus,
                DynamicLaneActivationMetrics> {
}
