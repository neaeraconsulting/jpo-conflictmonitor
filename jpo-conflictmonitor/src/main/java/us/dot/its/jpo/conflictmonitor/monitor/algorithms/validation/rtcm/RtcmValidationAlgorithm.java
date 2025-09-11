package us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.rtcm;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.Algorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.rtcm.RtcmMinimumDataAggregationAlgorithm;

public interface RtcmValidationAlgorithm
    extends Algorithm<RtcmValidationParameters> {

    void setMinimumDataAggregationAlgorithm(RtcmMinimumDataAggregationAlgorithm minimumDataAggregationAlgorithm);
}
