package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.spat.SpatMinimumDataAggregationAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.spat.SpatMinimumDataAggregationStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.timestamp_delta.spat.SpatTimestampDeltaAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.timestamp_delta.spat.SpatTimestampDeltaStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.spat.SpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.spat.SpatValidationStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate.SpatBroadcastRateEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.SpatMinimumDataEvent;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.ValidationConstants.CTI_4501_V2_SPAT_VALIDATION_ALGORITHM;

/**
 * Assessments/validations for SPAT messages.
 * Implements the new standard for measuring Broadcast Rate in CTI-4501 V2.
 * <p>Reads {@link ProcessedSpat} messages.
 * <p>Produces {@link SpatBroadcastRateEvent}s and {@link SpatMinimumDataEvent}s
 */
@Component(CTI_4501_V2_SPAT_VALIDATION_ALGORITHM)
public class SpatValidationTopologyCti4501V2
        extends BaseValidationTopology<SpatValidationParameters>
        implements SpatValidationStreamsAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(SpatValidationTopologyCti4501V2.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    private static final String LATEST_TIMESTAMP_STORE = "latest-timestamp-store";

    SpatTimestampDeltaStreamsAlgorithm timestampDeltaAlgorithm;
    SpatMinimumDataAggregationStreamsAlgorithm minimumDataAggregationAlgorithm;

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

    @Override
    public Topology buildTopology() {
        var builder = new StreamsBuilder();

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
                        logger.info("SpatMinimumDataEvent {}", key);
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

        // Broadcast Rate Criteria in CTI-4501 v2 draft:
        // - SPAT messages broadcast every 100 ms +/- 25 ms
        // - Any 10 messages broadcast within 1 s +/- 25 ms
        // - Planned addendum not in the draft: Conformant if 90% of message pairs and groups of 10 messages
        // meet the criteria over 1 hour, and no gaps greater than 300 ms between pairs.

        // Use a tumbling window to sort out-of-order spats by timestamp
        KStream<RsuIntersectionKey, Long> sortedSpatTimestamps =
                processedSpatStream
                        // Map the values to the timestamp
                        .process(() -> new ContextualProcessor<RsuIntersectionKey, ProcessedSpat, RsuIntersectionKey, Long>() {
                            @Override
                            public void process(Record<RsuIntersectionKey, ProcessedSpat> record) {
                                context().forward(new Record<>(record.key(), record.timestamp(), record.timestamp()));
                            }
                        })
                        .groupByKey(
                                Grouped.with(
                                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                                        Serdes.Long())
                        )
                        .windowedBy(
                                // Tumbling window
                                TimeWindows
                                        .ofSizeAndGrace(
                                                Duration.ofSeconds(parameters.getV2BroadcastRateBufferSizeSeconds()),
                                                Duration.ofMillis(parameters.getV2BroadcastRateBufferGracePeriodMs()))
                        )
                        .aggregate(
                                TimestampBuffer::new,
                                (key, timestamp, aggregate) -> {
                                    aggregate.add(timestamp);
                                    return aggregate;
                                },
                                Materialized.<RsuIntersectionKey, TimestampBuffer, WindowStore<Bytes, byte[]>>as("spat-buffer")
                                        .withKeySerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey())
                                        .withValueSerde(JsonSerdes.TimestampBuffer())
                        )
                        .suppress(
                                Suppressed.untilWindowCloses(BufferConfig.unbounded())
                        )
                        .toStream()
                        .map((windowedKey, buffer) -> {
                            // sort the buffered timestamps
                            Collections.sort(buffer);

                            // Change the windowed key back to a normal key
                            return KeyValue.pair(windowedKey.key(), buffer);
                        })
                        .flatMapValues(buffer -> {
                            // Unwrap buffered timestamps, now in order
                            return buffer;
                        });


        // Table holds the 10 most recent spats for each intersection
        KTable<RsuIntersectionKey, TimestampBoundedQueue> timestampAggTable =
            sortedSpatTimestamps
                .groupByKey(
                    Grouped.with(
                            us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                            Serdes.Long())
                )
                .aggregate(
                        TimestampBoundedQueue::new,
                        (key, timestamp, aggregate) -> {
                            aggregate.add(timestamp);
                            return aggregate;
                        },
                        Materialized.<RsuIntersectionKey, TimestampBoundedQueue, KeyValueStore<Bytes, byte[]>>as("spat-criterion-store")
                                .withKeySerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey())
                                .withValueSerde(JsonSerdes.TimestampBoundedQueue())
                );

        // Check the broadcast rate criteria
        // for each pair and each group of 10 consecutive spats
        timestampAggTable
                .toStream()
                .map((key, agg) -> {
                    var event = new SpatBroadcastRateEvent();
                });


//        KStream<RsuIntersectionKey, SpatBroadcastRateEvent> eventStream = countStream
//                .filter((windowedKey, value) -> {
//                    if (value != null) {
//                        long counts = value.longValue();
//                        return (counts < parameters.getLowerBound() || counts > parameters.getUpperBound());
//                    }
//                    return false;
//                })
//                .map((windowedKey, counts) -> {
//                    // Generate an event
//                    SpatBroadcastRateEvent event = new SpatBroadcastRateEvent();
//                    event.setSource(windowedKey.key().toString());
//                    event.setIntersectionID(windowedKey.key().getIntersectionId());
//                    event.setRoadRegulatorID(-1);
//                    event.setTopicName(parameters.getInputTopicName());
//                    ProcessingTimePeriod timePeriod = new ProcessingTimePeriod();
//
//                    // Grab the timestamps from the time window
//                    timePeriod.setBeginTimestamp(windowedKey.window().startTime().toEpochMilli());
//                    timePeriod.setEndTimestamp(windowedKey.window().endTime().toEpochMilli());
//                    event.setTimePeriod(timePeriod);
//                    event.setNumberOfMessages(counts != null ? counts.intValue() : -1);
//
//                    // Change the windowed key back to a normal key
//                    return KeyValue.pair(windowedKey.key(), event);
//                });
//
//        if (parameters.isDebug()) {
//            eventStream = eventStream.peek((key, event) -> {
//                logger.info("SPAT Broadcast Rate {}, {}", key, event);
//            });
//        }
//
//        eventStream.to(parameters.getBroadcastRateTopicName(),
//                Produced.with(
//                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
//                        JsonSerdes.SpatBroadcastRateEvent(),
//                        new IntersectionIdPartitioner<RsuIntersectionKey, SpatBroadcastRateEvent>())
//        );

        return builder.build(streamsProperties);
    }


}
