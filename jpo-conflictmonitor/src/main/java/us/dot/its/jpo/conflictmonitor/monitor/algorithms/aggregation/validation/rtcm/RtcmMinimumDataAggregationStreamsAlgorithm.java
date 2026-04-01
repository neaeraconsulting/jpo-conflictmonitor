package us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.rtcm;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.AggregationStreamsAlgorithmInterface;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.RtcmMinimumDataEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.RtcmMinimumDataEventAggregation;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;

public interface RtcmMinimumDataAggregationStreamsAlgorithm
    extends
        RtcmMinimumDataAggregationAlgorithm,
        AggregationStreamsAlgorithmInterface<
                RsuStationIdKey,
                        RtcmMinimumDataEvent,
                        RtcmMinimumDataEventAggregation> {
}
