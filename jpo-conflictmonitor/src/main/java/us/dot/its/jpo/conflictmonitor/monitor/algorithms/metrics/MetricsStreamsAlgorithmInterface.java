package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.Event;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.Metrics;

/**
 * Streams implementation of a metrics algorithm that plugs into a topology, reads a stream of events or other messages,
 * and produces a stream of metrics.
 */
public interface MetricsStreamsAlgorithmInterface<TEventKey, TMetricKey, TEvent, TMetric extends Metrics<TMetricKey>> {

    KStream<TMetricKey, TMetric> buildTopology(StreamsBuilder builder, KStream<TEventKey, TEvent> inputStream);

}
