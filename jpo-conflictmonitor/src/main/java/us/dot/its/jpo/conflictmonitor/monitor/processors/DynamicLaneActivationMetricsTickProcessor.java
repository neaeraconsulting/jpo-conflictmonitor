package us.dot.its.jpo.conflictmonitor.monitor.processors;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.CommonMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.revocable_enabled_lane_alignment.RevocableLaneStatus;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation.DynamicLaneActivationMetrics;
import us.dot.its.jpo.conflictmonitor.monitor.processors.metrics.TickProcessor;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;

public class DynamicLaneActivationMetricsTickProcessor
    extends TickProcessor<RsuIntersectionKey, RevocableLaneStatus> {

    public DynamicLaneActivationMetricsTickProcessor(CommonMetricsParameters params, boolean isDebug, String timestampStoreName) {
        super(params, isDebug, new DynamicLaneActivationMetrics().getName(), timestampStoreName);
    }

    @Override
    public RevocableLaneStatus tickValue() {
        var tick = new RevocableLaneStatus();
        tick.setTick(true);
        return tick;
    }
}
