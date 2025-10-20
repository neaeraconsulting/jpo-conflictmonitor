package us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.Algorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsAlgorithm;

public interface PriorityPreemptionRequestAlgorithm
        extends Algorithm<PriorityPreemptionRequestParameters> {
    PriorityRequestMetricsAlgorithm getPriorityRequestMetricsAlgorithm();
    void setPriorityRequestMetricsAlgorithm(PriorityRequestMetricsAlgorithm priorityRequestMetricsAlgorithm);
}
