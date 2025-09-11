package us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.rtcm;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.AggregationAlgorithmInterface;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.RtcmMinimumDataEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.RtcmMinimumDataEventAggregation;

public interface RtcmMinimumDataAggregationAlgorithm
    extends AggregationAlgorithmInterface<
        RtcmMinimumDataEvent,
        RtcmMinimumDataEventAggregation> {
}
