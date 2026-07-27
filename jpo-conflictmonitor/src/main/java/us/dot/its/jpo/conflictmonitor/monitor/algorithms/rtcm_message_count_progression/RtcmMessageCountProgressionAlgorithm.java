package us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.Algorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.rtcm_message_count_progression.RtcmMessageCountProgressionAggregationAlgorithm;

/**
 * Base interface for RTCM Message Count Progression algorithms
 */
public interface RtcmMessageCountProgressionAlgorithm
    extends Algorithm<RtcmMessageCountProgressionParameters> {

    /**
     * Set the aggregation algorithm for RTCM Message Count Progression
     * @param aggregationAlgorithm the aggregation algorithm
     */
    void setAggregationAlgorithm(RtcmMessageCountProgressionAggregationAlgorithm aggregationAlgorithm);

}
