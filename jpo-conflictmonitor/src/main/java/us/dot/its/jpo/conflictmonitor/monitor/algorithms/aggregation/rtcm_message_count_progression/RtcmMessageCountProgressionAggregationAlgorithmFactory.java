package us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.rtcm_message_count_progression;

public interface RtcmMessageCountProgressionAggregationAlgorithmFactory {
    RtcmMessageCountProgressionAggregationAlgorithm getAlgorithm(String algorithmName);
}
