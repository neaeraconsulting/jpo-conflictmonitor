package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.Event;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.Metrics;

/**
 * Streams implementation of a metrics algorithm that plugs into a topology that produces an event.
 */
public interface MetricsStreamsAlgorithmInterface<TKey, TEvent extends Event, TMetric extends Metrics<TKey>> {

    KStream<TKey, TMetric> buildTopology(StreamsBuilder builder, KStream<TKey, TEvent> inputStream);

}
