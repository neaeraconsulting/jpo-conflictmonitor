package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.spat;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.spat.SpatMinimumDataAggregationAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.spat.SpatMinimumDataAggregationStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.timestamp_delta.spat.SpatTimestampDeltaAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.timestamp_delta.spat.SpatTimestampDeltaStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.spat.SpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.spat.SpatValidationStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.SpatMinimumDataEvent;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.BaseValidationTopology;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.TimestampExtractorForBroadcastRate;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

@Slf4j
public abstract class BaseSpatValidationTopology
        extends BaseValidationTopology<SpatValidationParameters>
        implements SpatValidationStreamsAlgorithm {

    private SpatTimestampDeltaStreamsAlgorithm timestampDeltaAlgorithm;
    private SpatMinimumDataAggregationStreamsAlgorithm minimumDataAggregationAlgorithm;

    @Override
    public SpatTimestampDeltaAlgorithm getTimestampDeltaAlgorithm() {
        return timestampDeltaAlgorithm;
    }

    @Override
    public void setTimestampDeltaAlgorithm(SpatTimestampDeltaAlgorithm timestampDeltaAlgorithm) {
        // Enforce the algorithm being a Streams algorithm
        if (timestampDeltaAlgorithm instanceof SpatTimestampDeltaStreamsAlgorithm timestampDeltaStreamsAlgorithm) {
            this.timestampDeltaAlgorithm = timestampDeltaStreamsAlgorithm;
        } else {
            throw new IllegalArgumentException("Algorithm is not an instance of SpatTimestampDeltaStreamsAlgorithm");
        }
    }

    @Override
    public void setMinimumDataAggregationAlgorithm(SpatMinimumDataAggregationAlgorithm minimumDataAggregationAlgorithm) {
        // Enforce the algorithm being a Streams algorithm
        if (minimumDataAggregationAlgorithm instanceof SpatMinimumDataAggregationStreamsAlgorithm minimumDataAggregationStreamsAlgorithm) {
            this.minimumDataAggregationAlgorithm = minimumDataAggregationStreamsAlgorithm;
        } else {
            throw new IllegalArgumentException("Algorithm is not an instance of SpatMinimumDataAggregationStreamsAlgorithm");
        }
    }

    @Override
    protected void validate() {
        super.validate();

        if (timestampDeltaAlgorithm == null) {
            throw new IllegalStateException("SpatTimestampDeltaAlgorithm is not set");
        }
    }

    protected KStream<RsuIntersectionKey, ProcessedSpat> buildMinimumDataSubtopology(StreamsBuilder builder) {

        KStream<RsuIntersectionKey, ProcessedSpat> processedSpatStream = builder
                .stream(parameters.getInputTopicName(),
                        Consumed.with(
                                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSpat())
                                .withTimestampExtractor(new TimestampExtractorForBroadcastRate())
                );

        // timestamp delta plugin after reading processed SPATs
        timestampDeltaAlgorithm.buildTopology(builder, processedSpatStream);


        // Extract validation info for Minimum Data events
        var minimumDataEventStream = processedSpatStream
                .filter((key, value) -> value != null && !value.isCti4501Conformant())
                .map((key, value) -> {
                    var minDataEvent = new SpatMinimumDataEvent();
                    var valMsgList = value.getValidationMessages();
                    var timestamp = TimestampExtractorForBroadcastRate.extractTimestamp(value);
                    populateMinDataEvent(key, minDataEvent, valMsgList, parameters.getRollingPeriodSeconds(),
                            timestamp);

                    return KeyValue.pair(key, minDataEvent);
                })
                .peek((key, value) -> {
                    if (parameters.isDebug()) {
                        log.info("SpatMinimumDataEvent {}", key);
                    }
                });


        // If aggregation is enabled, don't send individual events to the topic
        // This is a read-only flag, so the subtopology for the unchosen option is not constructed at all
        if (parameters.isAggregateMinimumDataEvents()) {
            // Aggregate
            minimumDataAggregationAlgorithm.buildTopology(builder, minimumDataEventStream);
        } else {
            // Dont' aggregate
            minimumDataEventStream
                    .to(parameters.getMinimumDataTopicName(),
                            Produced.with(
                                    us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                                    JsonSerdes.SpatMinimumDataEvent(),
                                    new IntersectionIdPartitioner<>())
                    );
        }
        return processedSpatStream;
    }
}
