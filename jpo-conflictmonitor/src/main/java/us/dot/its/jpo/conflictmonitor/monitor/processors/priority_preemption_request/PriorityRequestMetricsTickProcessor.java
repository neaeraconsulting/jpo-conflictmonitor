package us.dot.its.jpo.conflictmonitor.monitor.processors.priority_preemption_request;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.CommonMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.IntersectionVehicleRequestStatus;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.IntersectionVehicleTypeKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.PriorityRequestMetrics;
import us.dot.its.jpo.conflictmonitor.monitor.processors.metrics.TickProcessor;

public class PriorityRequestMetricsTickProcessor
    extends TickProcessor<IntersectionVehicleTypeKey, IntersectionVehicleRequestStatus> {

    public PriorityRequestMetricsTickProcessor(CommonMetricsParameters params, boolean isDebug, String timestampStoreName) {
        super(params, isDebug, new PriorityRequestMetrics().getName(), timestampStoreName);
    }

    @Override
    public IntersectionVehicleRequestStatus tickValue() {
        return new IntersectionVehicleRequestStatus(null, TICK);
    }
}
