package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.TimestampType;
import us.dot.its.jpo.conflictmonitor.monitor.models.assessments.broadcast_rate.MapBroadcastRateAssessment;
import us.dot.its.jpo.conflictmonitor.monitor.models.assessments.broadcast_rate.RsuIntersectionTimestampTypeKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate.MapBroadcastRateEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.MapMinimumDataEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.notifications.broadcast_rate.MapBroadcastRateNotification;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.TimestampBoundedQueue;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.TimestampBuffer;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.LineString;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;
import us.dot.its.jpo.geojsonconverter.serialization.deserializers.JsonDeserializer;
import us.dot.its.jpo.geojsonconverter.serialization.serializers.JsonSerializer;
import us.dot.its.jpo.geojsonconverter.standards.MapStandard;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.ValidationConstants.CTI_4501_V2_MAP_VALIDATION_ALGORITHM;

/**
 * Assessments/validations for MAP messages.
 * Implements the new standard for measuring Broadcast Rate in CTI-4501 V2.
 * <p>Reads {@link ProcessedMap} messages.
 * <p>Produces {@link MapBroadcastRateEvent}s, {@link MapBroadcastRateNotification}s {@link MapMinimumDataEvent}s
 */
@Component(CTI_4501_V2_MAP_VALIDATION_ALGORITHM)
public class MapValidationTopologyV2 extends BaseMapValidationTopology {

    private static final Logger logger = LoggerFactory.getLogger(MapValidationTopologyV2.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public Topology buildTopology() {
        var builder = new StreamsBuilder();
        KStream<RsuIntersectionKey, ProcessedMap<LineString>> processedMapStream = buildMinimumDataSubtopology(builder);

        // Broadcast Rate Criteria in CTI-4501 v2 draft:
        // - MAP messages broadcast every 1 second +/- 25 ms
        // - Any 10 messages broadcast within 10 s +/- 25 ms
        // - Planned addendum not in the draft: Conformant if 90% of message pairs and groups of 10 messages
        // meet the criteria over 1 hour.

        // Only use odeReceivedAt for MAPs, since MAPs aren't required to have an embedded timestamp.

        // Sort by odeReceivedAt timestamps in order
        KStream<RsuIntersectionKey, Long> unsortedOdeReceivedAtTimestamps
                = processedMapStreamToOdeReceivedAtStream(processedMapStream);
        KStream<RsuIntersectionKey, Long> sortedOdeReceivedAtTimestamps
                = buildSortedMapTimestampStream(unsortedOdeReceivedAtTimestamps);

        // Aggregate odeReceivedat timestamps for comparison checks
        KTable<RsuIntersectionKey, TimestampBoundedQueue> odeReceivedAtAggTable
                = buildTimestampAggTable(sortedOdeReceivedAtTimestamps);

        // Produce events
        KStream<RsuIntersectionKey, TimestampedEvents> odeReceivedAtEventStream
                = buildEventStream(odeReceivedAtAggTable);
        publishEvents(odeReceivedAtEventStream);

        // Do assessments over a longer time period for pass/fail with 90% tolerance
        // Notifications are always sent for either pass/fail so clients can get complete assessment statistics.
        KStream<RsuIntersectionTimestampTypeKey, MapBroadcastRateAssessment> assessmentStream
                = buildAssessmentStream(sortedOdeReceivedAtTimestamps, odeReceivedAtEventStream
        );

        publishNotifications(assessmentStream);

        return builder.build(streamsProperties);
    }

    private KStream<RsuIntersectionKey, Long>
    processedMapStreamToOdeReceivedAtStream(KStream<RsuIntersectionKey, ProcessedMap<LineString>> processedMapStream) {
        return processedMapStream.process(() -> new ContextualProcessor<RsuIntersectionKey, ProcessedMap<LineString>, RsuIntersectionKey, Long>() {
            @Override
            public void process(Record<RsuIntersectionKey, ProcessedMap<LineString>> record) {
                // Ensure we are using odeRecievedAt as timestamp
                var map = record.value();
                if (map == null || map.getProperties() == null || map.getProperties().getOdeReceivedAt() == null) {
                    getLogger().info("Missing odeReceivedAt timestamp, dropping map for key {}", record.key());
                    return;
                }
                long timestamp = map.getProperties().getOdeReceivedAt().toInstant().toEpochMilli();
                context().forward(new Record<>(record.key(), timestamp, timestamp));
            }
        });
    }

    // Use a tumbling window to sort out-of-order spats by timestamp
    private KStream<RsuIntersectionKey, Long> buildSortedMapTimestampStream(
            KStream<RsuIntersectionKey, Long> unsortedMapTimestamps) {
        Duration windowSize = Duration.ofSeconds(parameters.getV2BroadcastRateBufferSizeSeconds());
        Duration gracePeriod = Duration.ofMillis(parameters.getV2BroadcastRateBufferGracePeriodMs());
        return unsortedMapTimestamps
                .groupByKey(
                        Grouped.with(
                                us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                                Serdes.Long())
                )
                .windowedBy(
                        TimeWindows.ofSizeAndGrace(windowSize, gracePeriod)
                )
                .aggregate(
                        TimestampBuffer::new,
                        (key, timestamp, aggregate) -> {
                            aggregate.add(timestamp);
                            return aggregate;
                        },
                        Materialized.<RsuIntersectionKey, TimestampBuffer>as(
                                        Stores.inMemoryWindowStore("map-received-at-buffer",
                                                windowSize.plus(gracePeriod), windowSize, false))
                                .withKeySerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey())
                                .withValueSerde(JsonSerdes.TimestampBuffer())
                                .withCachingDisabled()
                                .withLoggingDisabled()
                )
                .suppress(
                        Suppressed.untilWindowCloses(BufferConfig.unbounded())
                )
                .toStream()
                .process(() -> new ContextualProcessor<>() {
                    @Override
                    public void process(Record<Windowed<RsuIntersectionKey>, TimestampBuffer> record) {
                        TimestampBuffer buffer = record.value();

                        // Sort the buffered timestamps
                        Collections.sort(buffer);

                        // Convert the windowed key to a normal key
                        RsuIntersectionKey key = record.key().key();

                        // Unwrap the buffered timestamps
                        for (Long timestamp : buffer) {
                            context().forward(new Record<>(key, timestamp, timestamp));
                        }
                    }
                });
    }

    // Table holds the 10 most recent MAPs for each intersection
    private KTable<RsuIntersectionKey, TimestampBoundedQueue> buildTimestampAggTable(
            KStream<RsuIntersectionKey, Long> sortedMapTimestamps) {
        return sortedMapTimestamps
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
                        Materialized.<RsuIntersectionKey, TimestampBoundedQueue>as(
                                        Stores.inMemoryKeyValueStore("map-ode-received-at-agg-buffer"))
                                .withKeySerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey())
                                .withValueSerde(JsonSerdes.TimestampBoundedQueue())
                                .withCachingDisabled()
                                .withLoggingDisabled()
                );
    }

    // Check the broadcast rate criteria for each pair and each group of 10 consecutive MAPs
    private KStream<RsuIntersectionKey, TimestampedEvents> buildEventStream(
            KTable<RsuIntersectionKey, TimestampBoundedQueue> timestampAggTable) {
        return timestampAggTable
                .toStream()
                .map((key, agg)
                        -> new KeyValue<>(key, toTimestampedEvents(key, agg)))
                .filter((key, optEvents) -> optEvents.isPresent())
                .mapValues(Optional::get);
    }

    private Optional<TimestampedEvents> toTimestampedEvents(RsuIntersectionKey key, TimestampBoundedQueue agg) {
        MapBroadcastRateEvent pairEvent = extractPairEvent(key, agg);
        MapBroadcastRateEvent durationEvent = extractDurationEvent(key, agg);
        long latest = agg.latest().orElse(0L);
        if ((pairEvent == null && durationEvent == null) || latest == 0) {
            return Optional.empty();
        }
        return Optional.of(new TimestampedEvents(latest, pairEvent, durationEvent));
    }

    private MapBroadcastRateEvent extractPairEvent(RsuIntersectionKey key, TimestampBoundedQueue agg) {
        Optional<long[]> pairOpt = agg.pair();
        if (pairOpt.isEmpty()) {
            return null;
        }
        long[] pair = pairOpt.get();
        long diff = pair[1] - pair[0];
        if (diff < parameters.getV2BroadcastRateLowerBoundPairSeparationMs()
                || diff > parameters.getV2BroadcastRateUpperBoundPairSeparationMs()) {
            return getEvent(key, pair[0], pair[1], 2);
        }
        return null;
    }

    private MapBroadcastRateEvent extractDurationEvent(RsuIntersectionKey key, TimestampBoundedQueue agg) {
        Optional<long[]> allOpt = agg.all();
        if (allOpt.isEmpty()) {
            return null;
        }
        long[] all = allOpt.get();
        long first = all[0];
        long last = all[all.length - 1];
        long diff = last - first;
        if (diff < parameters.getV2BroadcastRateLowerBoundDurationPer10MessagesMs()
                || diff > parameters.getV2BroadcastRateUpperBoundDurationPer10MessagesMs()) {
            return getEvent(key, first, last, agg.numberOfMessagesForDuration());
        }
        return null;
    }

    private void publishEvents(KStream<RsuIntersectionKey, TimestampedEvents> eventStream) {
        eventStream
                .filter((key, value) -> value != null && (value.durationEvent() != null || value.pairEvent() != null))
                .flatMapValues(this::toEventList)
                .to(parameters.getBroadcastRateTopicName(),
                        Produced.with(
                                us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                                JsonSerdes.MapBroadcastRateEvent(),
                                new IntersectionIdPartitioner<>())
                );
    }

    private List<MapBroadcastRateEvent> toEventList(TimestampedEvents value) {
        var events = new ArrayList<MapBroadcastRateEvent>();
        if (value.pairEvent() != null) {
            events.add(value.pairEvent());
        }
        if (value.durationEvent() != null) {
            events.add(value.durationEvent());
        }
        return events;
    }

    private KStream<RsuIntersectionTimestampTypeKey, MapBroadcastRateAssessment> buildAssessmentStream(
            KStream<RsuIntersectionKey, Long> sortedMapTimestamps,
            KStream<RsuIntersectionKey, TimestampedEvents> eventStream) {
        Duration joinGracePeriod = Duration.ofMillis(parameters.getV2BroadcastRateAssessmentWindowGracePeriodMs());
        return sortedMapTimestamps
                .leftJoin(eventStream,
                        (timestamp, events)
                                -> (events != null) ? events : new TimestampedEvents(timestamp, null, null),
                        JoinWindows.ofTimeDifferenceAndGrace(Duration.ZERO, joinGracePeriod),
                        StreamJoined
                                .with(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                                        Serdes.Long(),
                                        TimestampedEvents.serde())
                                .withStoreName("map-ode-received-at-join-store")
                                .withThisStoreSupplier(Stores.inMemoryWindowStore(
                                        "map-ode-received-at-join-store" + "-this", joinGracePeriod, Duration.ZERO, true))
                                .withOtherStoreSupplier(Stores.inMemoryWindowStore(
                                        "map-ode-received-at-join-store" + "-other", joinGracePeriod, Duration.ZERO, true))
                                .withLoggingDisabled()
                )
                .groupByKey(
                        Grouped.with(
                                us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                                TimestampedEvents.serde())
                )
                .windowedBy(
                        TimeWindows.ofSizeAndGrace(
                                Duration.of(parameters.getV2BroadcastRateAssessmentWindowDuration(),
                                        parameters.getV2BroadcastRateAssessmentWindowDurationUnits()),
                                Duration.ofMillis(parameters.getV2BroadcastRateAssessmentWindowGracePeriodMs()))
                )
                .aggregate(
                        MapBroadcastRateAssessment::new,
                        (key, events, assessment) -> updateAssessment(events, assessment),
                        Materialized.<RsuIntersectionKey, MapBroadcastRateAssessment, WindowStore<Bytes, byte[]>>as("map-ode-received-at-assessment-buffer-store")
                                .withKeySerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey())
                                .withValueSerde(JsonSerdes.MapBroadcastRateAssessment())
                                .withCachingDisabled()
                                .withLoggingDisabled()
                )
                .suppress(
                        Suppressed.untilWindowCloses(BufferConfig.unbounded())
                )
                .toStream()
                .map((windowedKey, assessment)
                        -> {
                    var intersectionKey = windowedKey.key();
                    var intersectionTimestampTypeKey
                            = new RsuIntersectionTimestampTypeKey(intersectionKey, TimestampType.ODE_RECEIVED_AT);
                    return new KeyValue<>(intersectionTimestampTypeKey,
                            finalizeAssessment(windowedKey, assessment));
                });
    }

    private MapBroadcastRateAssessment updateAssessment(TimestampedEvents events, MapBroadcastRateAssessment assessment) {
        assessment.setNumberOfMessages(assessment.getNumberOfMessages() + 1);
        if (events.pairEvent() != null) {
            assessment.setNumberOfPairViolations(assessment.getNumberOfPairViolations() + 1);
            var timePeriod = events.pairEvent().getTimePeriod();
            long first = timePeriod.getBeginTimestamp();
            long second = timePeriod.getEndTimestamp();
            long diff = second - first;
            if (diff > assessment.getMaxPairSeparationMs()) {
                assessment.setMaxPairSeparationMs((int) diff);
            }
        }
        if (events.durationEvent() != null) {
            assessment.setNumberOfDurationViolations(assessment.getNumberOfDurationViolations() + 1);
        }
        return assessment;
    }

    private MapBroadcastRateAssessment finalizeAssessment(
            Windowed<RsuIntersectionKey> windowedKey,
            MapBroadcastRateAssessment assessment) {
        RsuIntersectionKey key = windowedKey.key();
        assessment.setIntersectionID(key.getIntersectionId());
        assessment.setRoadRegulatorID(key.getRegion());
        assessment.setSource(key.toString());
        assessment.setTimestampType(TimestampType.ODE_RECEIVED_AT);
        var timePeriod = new ProcessingTimePeriod();
        timePeriod.setBeginTimestamp(windowedKey.window().start());
        timePeriod.setEndTimestamp(windowedKey.window().end());
        assessment.setTimePeriod(timePeriod);
        assessment.setPercentToPass(parameters.getV2BroadcastRateConformancePercent());
        assessment.setAssessmentGeneratedAt(Instant.now().toEpochMilli());
        return assessment;
    }

    private void publishNotifications(KStream<RsuIntersectionTimestampTypeKey, MapBroadcastRateAssessment> assessmentStream) {
        assessmentStream
                .mapValues(this::toNotification)
                .to(parameters.getBroadcastRateNotificationTopicName(),
                        Produced.with(
                                JsonSerdes.RsuIntersectionTimestampTypeKey(),
                                JsonSerdes.MapBroadcastRateNotification(),
                                new IntersectionIdPartitioner<>()));
    }

    private MapBroadcastRateNotification toNotification(MapBroadcastRateAssessment assessment) {
        var notification = new MapBroadcastRateNotification();
        notification.setAssessment(assessment);
        notification.setNotificationText("MAP Broadcast Rate Notification, with periodic broadcast rate assessment report");
        notification.setNotificationHeading(
                "MAP Broadcast Rate Assessment: " + (notification.isPass() ? "Pass" : "Fail"));
        return notification;
    }

    private MapBroadcastRateEvent getEvent(RsuIntersectionKey key, long first, long last, int numberOfMessages) {
        var event = new MapBroadcastRateEvent();
        event.setIntersectionID(key.getIntersectionId());
        event.setRoadRegulatorID(key.getRegion());
        event.setSource(key.getRsuId());
        event.setTopicName(parameters.getInputTopicName());
        event.setStandard(MapStandard.CTI4501_V2_DRAFT);
        event.setTimestampType(TimestampType.ODE_RECEIVED_AT);
        event.setNumberOfMessages(numberOfMessages);
        var period = new ProcessingTimePeriod();
        period.setBeginTimestamp(first);
        period.setEndTimestamp(last);
        event.setTimePeriod(period);
        return event;
    }

    private record TimestampedEvents(
            long timestamp,
            MapBroadcastRateEvent pairEvent,
            MapBroadcastRateEvent durationEvent) {
        public static Serde<TimestampedEvents> serde() {
            return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(TimestampedEvents.class));
        }
    }
}
