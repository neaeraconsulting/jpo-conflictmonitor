package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.MetricsStreamsAlgorithmInterface;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.IntersectionVehicleTypeKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.PriorityRequestMetrics;

public interface PriorityRequestMetricsStreamsAlgorithm
    extends PriorityRequestMetricsAlgorithm,
        MetricsStreamsAlgorithmInterface<
            IntersectionVehicleTypeKey,
            PriorityPreemptionRequestEvent,
            PriorityRequestMetrics> {
}
