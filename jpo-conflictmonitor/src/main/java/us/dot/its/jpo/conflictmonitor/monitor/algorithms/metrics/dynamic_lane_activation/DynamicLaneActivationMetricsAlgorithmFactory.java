package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation;

public interface DynamicLaneActivationMetricsAlgorithmFactory {
    DynamicLaneActivationMetricsAlgorithm getAlgorithm(String algorithmName);
}
