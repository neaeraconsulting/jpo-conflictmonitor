package us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.rtcm;

public interface RtcmMinimumDataAggregationAlgorithmFactory {
    RtcmMinimumDataAggregationAlgorithm getAlgorithm(String algorithmName);
}
