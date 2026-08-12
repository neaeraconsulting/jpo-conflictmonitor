package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.spat;

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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.models.assessments.broadcast_rate.SpatBroadcastRateAssessment;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate.SpatBroadcastRateEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.SpatMinimumDataEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.notifications.broadcast_rate.SpatBroadcastRateNotification;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.TimestampBoundedQueue;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.TimestampBuffer;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;
import us.dot.its.jpo.geojsonconverter.serialization.deserializers.JsonDeserializer;
import us.dot.its.jpo.geojsonconverter.serialization.serializers.JsonSerializer;
import us.dot.its.jpo.geojsonconverter.standards.SpatStandard;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.ValidationConstants.CTI_4501_V2_SPAT_VALIDATION_ALGORITHM;

/**
 * Assessments/validations for SPAT messages.
 * Implements the new standard for measuring Broadcast Rate in CTI-4501 V2.
 * <p>Reads {@link ProcessedSpat} messages.
 * <p>Produces {@link SpatBroadcastRateEvent}s and {@link SpatMinimumDataEvent}s
 */
@Component(CTI_4501_V2_SPAT_VALIDATION_ALGORITHM)
public class SpatValidationTopologyV2 extends BaseSpatValidationTopology {

    private static final Logger logger = LoggerFactory.getLogger(SpatValidationTopologyV2.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public Topology buildTopology() {
        var builder = new StreamsBuilder();
        KStream<RsuIntersectionKey, ProcessedSpat> processedSpatStream = buildMinimumDataSubtopology(builder);

        // Broadcast Rate Criteria in CTI-4501 v2 draft:
        // - SPAT messages broadcast every 100 ms +/- 25 ms
        // - Any 10 messages broadcast within 1 s +/- 25 ms
        // - Planned addendum not in the draft: Conformant if 90% of message pairs and groups of 10 messages
        // meet the criteria over 1 hour, and no gaps greater than 300 ms between pairs.

        KStream<RsuIntersectionKey, Long> sortedSpatTimestamps = buildSortedSpatTimestampStream(processedSpatStream);
        KTable<RsuIntersectionKey, TimestampBoundedQueue> timestampAggTable = buildTimestampAggTable(sortedSpatTimestamps);
        KStream<RsuIntersectionKey, EventOrNonEvent> eventStream = buildEventStream(timestampAggTable);

        publishEvents(eventStream);

        // Do assessments over a longer time period for pass/fail with 90% tolerance
        // Event stream includes events and non-events, to keep stream time moving along in the absence of events
        KStream<RsuIntersectionKey, SpatBroadcastRateAssessment> assessmentStream = buildAssessmentStream(eventStream);

        // Send notifications for the assessments
        // Always sends notifications regardless if the assessment passes or fails, so clients
        // can always see the assessment statistics.
        publishNotifications(assessmentStream);

        return builder.build(streamsProperties);
    }

    // Use a tumbling window to sort out-of-order spats by timestamp
    private KStream<RsuIntersectionKey, Long> buildSortedSpatTimestampStream(
            KStream<RsuIntersectionKey, ProcessedSpat> processedSpatStream) {
        return processedSpatStream
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
    }

    // Table holds the 10 most recent spats for each intersection
    private KTable<RsuIntersectionKey, TimestampBoundedQueue> buildTimestampAggTable(
            KStream<RsuIntersectionKey, Long> sortedSpatTimestamps) {
        return sortedSpatTimestamps
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
    }

    // Check the broadcast rate criteria for each pair and each group of 10 consecutive spats
    private KStream<RsuIntersectionKey, EventOrNonEvent> buildEventStream(
            KTable<RsuIntersectionKey, TimestampBoundedQueue> timestampAggTable) {
        return timestampAggTable
                .toStream()
                .map((key, agg) -> new KeyValue<>(key, toEventOrNonEvent(key, agg)));
    }

    private EventOrNonEvent toEventOrNonEvent(RsuIntersectionKey key, TimestampBoundedQueue agg) {
        SpatBroadcastRateEvent pairEvent = extractPairEvent(key, agg);
        SpatBroadcastRateEvent durationEvent = extractDurationEvent(key, agg);
        long latest = agg.latest().orElse(0L);
        return new EventOrNonEvent(latest, pairEvent, durationEvent);
    }

    private SpatBroadcastRateEvent extractPairEvent(RsuIntersectionKey key, TimestampBoundedQueue agg) {
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

    private SpatBroadcastRateEvent extractDurationEvent(RsuIntersectionKey key, TimestampBoundedQueue agg) {
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

    private void publishEvents(KStream<RsuIntersectionKey, EventOrNonEvent> eventStream) {
        eventStream
                // Filter out non-events, only send actual events to the output topic
                .filter((key, value) -> value != null && (value.durationEvent() != null || value.pairEvent() != null))
                .flatMapValues(this::toEventList)
                .to(parameters.getBroadcastRateTopicName(),
                        Produced.with(
                                us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                                JsonSerdes.SpatBroadcastRateEvent(),
                                new IntersectionIdPartitioner<>())
                );
    }

    private List<SpatBroadcastRateEvent> toEventList(EventOrNonEvent value) {
        var events = new ArrayList<SpatBroadcastRateEvent>();
        if (value.pairEvent() != null) {
            events.add(value.pairEvent());
        }
        if (value.durationEvent() != null) {
            events.add(value.durationEvent());
        }
        return events;
    }

    private KStream<RsuIntersectionKey, SpatBroadcastRateAssessment> buildAssessmentStream(
            KStream<RsuIntersectionKey, EventOrNonEvent> eventStream) {
        return eventStream
                .groupByKey(
                        Grouped.with(
                                us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                                EventOrNonEvent.serde())
                )
                .windowedBy(
                        TimeWindows.ofSizeAndGrace(
                                Duration.of(parameters.getV2BroadcastRateAssessmentWindowDuration(),
                                        parameters.getV2BroadcastRateAssessmentWindowDurationUnits()),
                                Duration.ofMillis(parameters.getV2BroadcastRateAssessmentWindowGracePeriodMs()))
                )
                .aggregate(
                        SpatBroadcastRateAssessment::new,
                        (key, events, assessment) -> updateAssessment(events, assessment),
                        Materialized.<RsuIntersectionKey, SpatBroadcastRateAssessment, WindowStore<Bytes, byte[]>>as("spat-assessment-buffer")
                                .withKeySerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey())
                                .withValueSerde(JsonSerdes.SpatBroadcastRateAssessment())
                )
                .suppress(
                        Suppressed.untilWindowCloses(BufferConfig.unbounded())
                )
                .toStream()
                .map((windowedKey, assessment) -> new KeyValue<>(windowedKey.key(), finalizeAssessment(windowedKey, assessment)));
    }

    private SpatBroadcastRateAssessment updateAssessment(EventOrNonEvent events, SpatBroadcastRateAssessment assessment) {
        assessment.setNumberOfSpats(assessment.getNumberOfSpats() + 1);
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

    private SpatBroadcastRateAssessment finalizeAssessment(
            Windowed<RsuIntersectionKey> windowedKey, SpatBroadcastRateAssessment assessment) {
        RsuIntersectionKey key = windowedKey.key();
        assessment.setIntersectionID(key.getIntersectionId());
        assessment.setRoadRegulatorID(key.getRegion());
        assessment.setSource(key.toString());
        var timePeriod = new ProcessingTimePeriod();
        timePeriod.setBeginTimestamp(windowedKey.window().start());
        timePeriod.setEndTimestamp(windowedKey.window().end());
        assessment.setTimePeriod(timePeriod);
        assessment.setMaxAllowedPairSeparationMs(parameters.getV2BroadcastRateMaxOutlierPairSeparationMs());
        assessment.setPercentToPass(parameters.getV2BroadcastRateConformancePercent());
        assessment.setAssessmentGeneratedAt(Instant.now().toEpochMilli());
        return assessment;
    }

    private void publishNotifications(KStream<RsuIntersectionKey, SpatBroadcastRateAssessment> assessmentStream) {
        assessmentStream
                .mapValues(this::toNotification)
                .to(parameters.getBroadcastRateNotificationTopicName(),
                        Produced.with(
                                us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                                JsonSerdes.SpatBroadcastRateNotification(),
                                new IntersectionIdPartitioner<>()));
    }

    private SpatBroadcastRateNotification toNotification(SpatBroadcastRateAssessment assessment) {
        var notification = new SpatBroadcastRateNotification();
        notification.setAssessment(assessment);
        notification.setNotificationText("SPaT Broadcast Rate Notification, with periodic broadcast rate assessment report");
        notification.setNotificationHeading(
                "SPaT Broadcast Rate Assessment: " + (notification.isPass() ? "Pass" : "Fail"));
        return notification;
    }

    private SpatBroadcastRateEvent getEvent(RsuIntersectionKey key, long first, long last, int numberOfMessages) {
        var event = new SpatBroadcastRateEvent();
        event.setIntersectionID(key.getIntersectionId());
        event.setRoadRegulatorID(key.getRegion());
        event.setSource(key.getRsuId());
        event.setTopicName(parameters.getInputTopicName());
        event.setStandard(SpatStandard.CTI4501_V2_DRAFT);
        // Expect this is 10
        event.setNumberOfMessages(numberOfMessages);
        var period = new ProcessingTimePeriod();
        period.setBeginTimestamp(first);
        period.setEndTimestamp(last);
        event.setTimePeriod(period);
        return event;
    }

    private record EventOrNonEvent(
            long timestamp,
            SpatBroadcastRateEvent pairEvent,
            SpatBroadcastRateEvent durationEvent){
        public static Serde<EventOrNonEvent> serde() {
            return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(EventOrNonEvent.class));
        }
    }
}
