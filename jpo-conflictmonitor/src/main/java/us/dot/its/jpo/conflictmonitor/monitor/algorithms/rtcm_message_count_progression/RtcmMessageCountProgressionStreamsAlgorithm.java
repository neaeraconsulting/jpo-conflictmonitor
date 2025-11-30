package us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.StreamsTopology;

/**
 * Interface for Kafka Streams implementations of the RTCM Message Count Progression
 * algorithm.
 */
public interface RtcmMessageCountProgressionStreamsAlgorithm
    extends RtcmMessageCountProgressionAlgorithm, StreamsTopology {
}
