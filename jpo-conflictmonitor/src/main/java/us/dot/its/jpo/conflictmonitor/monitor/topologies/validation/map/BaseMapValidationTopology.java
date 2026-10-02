package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.map;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.map.MapMinimumDataAggregationAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.map.MapMinimumDataAggregationStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.timestamp_delta.map.MapTimestampDeltaAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.timestamp_delta.map.MapTimestampDeltaStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.map.MapValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.map.MapValidationStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.IntersectionRegion;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.MapMinimumDataEvent;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.BaseValidationTopology;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.TimestampExtractorForBroadcastRate;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.LineString;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;

@Slf4j
public abstract class BaseMapValidationTopology
        extends BaseValidationTopology<MapValidationParameters>
        implements MapValidationStreamsAlgorithm {

    private MapTimestampDeltaStreamsAlgorithm timestampDeltaAlgorithm;
    private MapMinimumDataAggregationStreamsAlgorithm minimumDataAggregationAlgorithm;

    @Override
    public MapTimestampDeltaAlgorithm getTimestampDeltaAlgorithm() {
        return timestampDeltaAlgorithm;
    }

    @Override
    public void setTimestampDeltaAlgorithm(MapTimestampDeltaAlgorithm timestampDeltaAlgorithm) {
        // Enforce the algorithm being a Streams algorithm
        if (timestampDeltaAlgorithm instanceof MapTimestampDeltaStreamsAlgorithm timestampDeltaStreamsAlgorithm) {
            this.timestampDeltaAlgorithm = timestampDeltaStreamsAlgorithm;
        } else {
            throw new IllegalArgumentException("algorithm is not an instance of MapTimestampDeltaStreamsAlgorithm");
        }
    }

    @Override
    public void setMinimumDataAggregationAlgorithm(MapMinimumDataAggregationAlgorithm minimumDataAggregationAlgorithm) {
        // Enforce the algorithm being a Streams algorithm
        if (minimumDataAggregationAlgorithm instanceof MapMinimumDataAggregationStreamsAlgorithm streamsAlgorithm) {
            this.minimumDataAggregationAlgorithm = streamsAlgorithm;
        } else {
            throw new IllegalArgumentException("Algorithm is not an instance of MapMinimumDataAggregationStreamsAlgorithm");
        }
    }

    @Override
    protected void validate() {
        super.validate();

        if (timestampDeltaAlgorithm == null) {
            throw new IllegalStateException("MapTimestampDeltaAlgorithm is not set.");
        }
    }

    protected KStream<RsuIntersectionKey, ProcessedMap<LineString>> buildMinimumDataSubtopology(StreamsBuilder builder) {

        KStream<RsuIntersectionKey, ProcessedMap<LineString>> processedMapStream = builder
                .stream(parameters.getInputTopicName(),
                        Consumed.with(
                                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedMapGeoJson())
                                .withTimestampExtractor(new TimestampExtractorForBroadcastRate())
                );

        timestampDeltaAlgorithm.buildTopology(builder, processedMapStream);

        // Extract validation info for Minimum Data events
        KStream<RsuIntersectionKey, MapMinimumDataEvent> minDataStream = processedMapStream
                .filter((key, value) -> value.getProperties() != null && !value.getProperties().getCti4501Conformant())
                .map((key, value) -> {
                    var minDataEvent = new MapMinimumDataEvent();
                    var valMsgList = value.getProperties().getValidationMessages();
                    var timestamp = TimestampExtractorForBroadcastRate.extractTimestamp(value);
                    populateMinDataEvent(key, minDataEvent, valMsgList, parameters.getRollingPeriodSeconds(),
                            timestamp);
                    return KeyValue.pair(key, minDataEvent);
                })
                .peek((key, value) -> {
                    var intersectionKey = IntersectionRegion.fromRsuIntersectionKey(key);
                    if (parameters.getDebug(intersectionKey)) {
                        log.info("MAP Min Data Event for intersection {}", intersectionKey);
                    }
                });

        // If aggregation is enabled, don't send individual events to the topic
        if (parameters.isAggregateMinimumDataEvents()) {
            // Aggregate
            minimumDataAggregationAlgorithm.buildTopology(builder, minDataStream);
        } else {
            // Dont' aggregate
            minDataStream.to(parameters.getMinimumDataTopicName(),
                    Produced.with(
                            us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                            JsonSerdes.MapMinimumDataEvent(),
                            new IntersectionIdPartitioner<RsuIntersectionKey, MapMinimumDataEvent>())
            );
        }

        return processedMapStream;
    }
}
