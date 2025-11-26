package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.BaseStreamsTopology;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.rtcm.RtcmMinimumDataAggregationAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.rtcm.RtcmMinimumDataAggregationStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.rtcm.RtcmValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.rtcm.RtcmValidationStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate.RtcmBroadcastRateEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.RtcmMinimumDataEvent;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.conflictmonitor.monitor.utils.RtcmUtils;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIdPartitioner;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;
import us.dot.its.jpo.geojsonconverter.pojos.ProcessedValidationMessage;


import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.ValidationConstants.DEFAULT_RTCM_VALIDATION_ALGORITHM;


/**
 * Assessments/validations for RTCM messages.
 */
@Component(DEFAULT_RTCM_VALIDATION_ALGORITHM)
@Slf4j
public class RtcmValidationTopology
        extends BaseStreamsTopology<RtcmValidationParameters>
        implements RtcmValidationStreamsAlgorithm {

    private static final String LATEST_TIMESTAMP_STORE = "latest-timestamp-store";

    RtcmMinimumDataAggregationStreamsAlgorithm minimumDataAggregationAlgorithm;

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    public void setMinimumDataAggregationAlgorithm(RtcmMinimumDataAggregationAlgorithm minimumDataAggregationAlgorithm) {
        // Enforce the algorithm being a Streams algorithm
        if (minimumDataAggregationAlgorithm instanceof RtcmMinimumDataAggregationStreamsAlgorithm streamsAlgorithm) {
            this.minimumDataAggregationAlgorithm = streamsAlgorithm;
        } else {
            throw new IllegalArgumentException("Algorithm is not an instance of RtcmMinimumDataAggregationStreamsAlgorithm");
        }
    }



    @Override
    public Topology buildTopology() {
        var builder = new StreamsBuilder();

        // Create state store for zero count
        var zeroCountStoreBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(LATEST_TIMESTAMP_STORE),
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey(),
                        Serdes.Long());

        builder.addStateStore(zeroCountStoreBuilder);

        var processedRtcmStream = builder
                .stream(parameters.getInputTopicName(),
                        Consumed.with(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey(),
                                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedRTCM())
                                .withTimestampExtractor(new TimestampExtractorForBroadcastRate()));

        // Extract validation info for Minimum Data events
        var minDataStream = processedRtcmStream
                .filter((key, processedRtcm) -> processedRtcm.getProperties() != null
                        && !processedRtcm.getProperties().isCti4501Conformant())
                .map((key, value) -> {
                    var minDataEvent = new RtcmMinimumDataEvent();
                    var valMsgList = value.getProperties().getValidationMessages();
                    var timestamp = TimestampExtractorForBroadcastRate.extractTimestamp(value);
                    populateMinDataEvent(key, minDataEvent, valMsgList, parameters.getRollingPeriodSeconds(),
                            timestamp);
                    return KeyValue.pair(key, minDataEvent);
                })
                .peek((key,  value) -> {
                    if (parameters.isDebug()) {
                        log.debug("RtcmMinimumDataEvent for key {}", key);
                    }
                });

        if (parameters.isAggregateMinimumDataEvents()) {
            minimumDataAggregationAlgorithm.buildTopology(builder, minDataStream);
        } else {
            minDataStream
                    .to(parameters.getMinimumDataTopicName(),
                            Produced.with(
                                    us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey(),
                                    JsonSerdes.RtcmMinimumDataEvent()));
        }


        processedRtcmStream.process(() -> new RtcmZeroRateChecker(
                parameters.getRollingPeriodSeconds(),
                parameters.getOutputIntervalSeconds(),
                parameters.getInputTopicName(),
                LATEST_TIMESTAMP_STORE
        ), LATEST_TIMESTAMP_STORE)
                .to(parameters.getBroadcastRateTopicName(),
                        Produced.with(
                                us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey(),
                                JsonSerdes.RtcmBroadcastRateEvent()
                        ).withStreamPartitioner(new RsuIdPartitioner<>()));

        var countStream =
                processedRtcmStream
                        .filter((key, rtcm) -> rtcm != null)
                        // Include a flag for MSM 4 messages in the key, since they need to be counted with different
                        // limits
                        .selectKey((key, rtcm) -> {
                            var newKey = new RsuStationIdRtcmTypeKey(key);
                            newKey.setIncludesMSMTypes(RtcmUtils.hasMSMTypes(rtcm));
                            return newKey;
                        })
                        .repartition(
                                Repartitioned.with(JsonSerdes.RsuStationIdRtcmTypeKey(),
                                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedRTCM())
                                        .withStreamPartitioner(new RsuIdPartitioner<>())) // Force repartitioning to keep same partition
                        .mapValues((value) -> 1)
                        .groupByKey(
                                Grouped.with(JsonSerdes.RsuStationIdRtcmTypeKey(),
                                        Serdes.Integer())
                        )
                        .windowedBy(
                                TimeWindows.ofSizeAndGrace(
                                        Duration.ofSeconds(parameters.getRollingPeriodSeconds()),
                                        Duration.ofMillis(parameters.getGracePeriodMilliseconds())))
                        .count(
                                Materialized.<RsuStationIdRtcmTypeKey, Long, WindowStore<Bytes, byte[]>>as("rtcm-counts")
                                        .withKeySerde(JsonSerdes.RsuStationIdRtcmTypeKey())
                                        .withValueSerde(Serdes.Long())
                        )
                        .suppress(
                                Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
                        )
                        .toStream();

        if (parameters.isDebug()) {
            countStream = countStream.peek((windowedKey, value) -> {
                log.info("RTCM Count {} {}",  windowedKey, value);
            });
        }

        var eventStream = countStream
                .filter((windowedKey, value) -> {
                    if (value != null) {
                        long counts = value;
                        // Use different bounds depending on whether any of the RTCMs contains MSMs
                        if (windowedKey.key().isIncludesMSMTypes()) {
                            return (counts < parameters.getMsmLowerBound() || counts > parameters.getMsmUpperBound());
                        } else {
                            return (counts < parameters.getLowerBound() || counts > parameters.getUpperBound());
                        }
                    }
                    return false;
                })
                .map((windowedKey, counts) -> {
                    var event = new RtcmBroadcastRateEvent();
                    event.setSource(windowedKey.key().getRsuId());
                    event.setStationId(windowedKey.key().getStationId());
                    event.setIntersectionID(-1);
                    event.setRoadRegulatorID(-1);
                    event.setTopicName(parameters.getInputTopicName());
                    var timePeriod = new ProcessingTimePeriod();
                    timePeriod.setBeginTimestamp(windowedKey.window().startTime().toEpochMilli());
                    timePeriod.setEndTimestamp(windowedKey.window().endTime().toEpochMilli());
                    event.setTimePeriod(timePeriod);
                    event.setNumberOfMessages(counts != null ? counts.intValue() : -1);
                    return KeyValue.pair(windowedKey.key().rsuStationIdKey(), event);
                });

        if (parameters.isDebug()) {
            eventStream = eventStream.peek((key, event) -> {
                log.info("RTCM Broadcast Rate {}, {}", key, event);
            });
        }

        eventStream.to(parameters.getBroadcastRateTopicName(),
                Produced.with(
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey(),
                        JsonSerdes.RtcmBroadcastRateEvent()
                ).withStreamPartitioner(new RsuIdPartitioner<>()));

        return builder.build();

    }

    private void populateMinDataEvent(
            RsuStationIdKey key,
            RtcmMinimumDataEvent minDataEvent,
            List<ProcessedValidationMessage> valMsgList,
            int rollingPeriodSeconds,
            long timestamp) {

        List<String> validationMessages =
                valMsgList
                        .stream()
                        .map(valMsg -> String.format("%s (%s)", valMsg.getMessage(), valMsg.getSchemaPath()))
                        .collect(Collectors.toList());

        minDataEvent.setMissingDataElements(validationMessages);
        if (key != null) {
            minDataEvent.setStationId(key.getStationId());
            minDataEvent.setSource(key.getRsuId());
        } else {
            log.warn("Key is null");
        }

        // Get the time window this event would be in without actually performing windowing
        // we just need to add the window timestamps to the event.

        // Use a tumbling window with no grace to avoid duplicates
        var timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(rollingPeriodSeconds));

        // Gets a map of all time windows this instant could be in
        Map<Long, TimeWindow> windows = timeWindows.windowsFor(timestamp);

        // Pick one (random map entry, but there should only be one for the tumbling window)
        TimeWindow window = windows.values().stream().findAny().orElse(null);
        if (window != null) {
            var timePeriod = new ProcessingTimePeriod();
            timePeriod.setBeginTimestamp(window.startTime().toEpochMilli());
            timePeriod.setEndTimestamp(window.endTime().toEpochMilli());
            minDataEvent.setTimePeriod(timePeriod);
        }
    }


}
