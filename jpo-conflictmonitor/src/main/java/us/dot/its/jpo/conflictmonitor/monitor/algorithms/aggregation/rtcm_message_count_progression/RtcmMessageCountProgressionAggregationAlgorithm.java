package us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.rtcm_message_count_progression;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.AggregationAlgorithmInterface;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.RtcmMessageCountProgressionEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.RtcmMessageCountProgressionEventAggregation;

public interface RtcmMessageCountProgressionAggregationAlgorithm
    extends AggregationAlgorithmInterface<
        RtcmMessageCountProgressionEvent,
        RtcmMessageCountProgressionEventAggregation> {
}
