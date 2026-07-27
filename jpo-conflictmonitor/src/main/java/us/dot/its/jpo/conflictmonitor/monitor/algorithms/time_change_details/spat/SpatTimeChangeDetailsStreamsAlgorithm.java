package us.dot.its.jpo.conflictmonitor.monitor.algorithms.time_change_details.spat;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

/**
 * Streams plugin for Spat Time Change Details; plugs into SpatValidationTopology
 * (single consume of ProcessedSpat).
 */
public interface SpatTimeChangeDetailsStreamsAlgorithm
        extends SpatTimeChangeDetailsAlgorithm {

    /**
     * @param inputStream event-time ProcessedSpat stream (utcTimeStamp)
     */
    void buildTopology(StreamsBuilder builder, KStream<RsuIntersectionKey, ProcessedSpat> inputStream);
}
