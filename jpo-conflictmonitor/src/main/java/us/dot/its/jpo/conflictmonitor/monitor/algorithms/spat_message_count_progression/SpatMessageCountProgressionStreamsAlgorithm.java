package us.dot.its.jpo.conflictmonitor.monitor.algorithms.spat_message_count_progression;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

/**
 * Streams plugin for SPaT message-count progression; plugs into SpatValidationTopology
 * (single consume of ProcessedSpat).
 */
public interface SpatMessageCountProgressionStreamsAlgorithm
        extends SpatMessageCountProgressionAlgorithm {

    /**
     * @param inputStream event-time ProcessedSpat stream (utcTimeStamp)
     */
    void buildTopology(StreamsBuilder builder, KStream<RsuIntersectionKey, ProcessedSpat> inputStream);
}
