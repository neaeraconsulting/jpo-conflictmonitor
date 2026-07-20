package us.dot.its.jpo.conflictmonitor.monitor.algorithms.revocable_enabled_lane_alignment;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.ConfigurableAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.revocable_enabled_lane_alignment.RevocableEnabledLaneAlignmentAggregationAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation.DynamicLaneActivationMetricsAlgorithm;

public interface RevocableEnabledLaneAlignmentAlgorithm
    extends ConfigurableAlgorithm<RevocableEnabledLaneAlignmentParameters> {

    void setAggregationAlgorithm(RevocableEnabledLaneAlignmentAggregationAlgorithm aggregationAlgorithm);

    void setDynamicLaneActivationMetricsAlgorithm(DynamicLaneActivationMetricsAlgorithm dynamicLaneActivationMetricsAlgorithm);
}
