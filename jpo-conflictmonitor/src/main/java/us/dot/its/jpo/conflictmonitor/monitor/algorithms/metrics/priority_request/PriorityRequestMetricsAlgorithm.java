package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.MetricsAlgorithmInterface;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.IntersectionVehicleTypeKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.PriorityRequestMetrics;

public interface PriorityRequestMetricsAlgorithm
    extends MetricsAlgorithmInterface<PriorityRequestMetrics, IntersectionVehicleTypeKey, PriorityRequestMetricsParameters> {
}
