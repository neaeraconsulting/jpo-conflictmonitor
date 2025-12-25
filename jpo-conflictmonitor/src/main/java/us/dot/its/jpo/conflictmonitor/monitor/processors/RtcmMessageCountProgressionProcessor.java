package us.dot.its.jpo.conflictmonitor.monitor.processors;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.DiffResult;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.MultiVersionedKeyQuery;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.*;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression.RtcmMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.RtcmMessageCountProgressionEvent;
import us.dot.its.jpo.conflictmonitor.monitor.utils.RtcmUtils;
import us.dot.its.jpo.conflictmonitor.monitor.utils.Timestamps;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;
import us.dot.its.jpo.geojsonconverter.serialization.deserializers.JsonDeserializer;
import us.dot.its.jpo.geojsonconverter.serialization.serializers.JsonSerializer;

import java.time.Duration;
import java.time.Instant;

@Slf4j
public class RtcmMessageCountProgressionProcessor
        extends ContextualProcessor<RsuStationIdKey, ProcessedRTCM, RsuStationIdKey, RtcmMessageCountProgressionEvent> {

    private VersionedKeyValueStore<RsuStationIdKey, ProcessedRTCM> bufferStore;

    private KeyValueStore<RsuStationIdKey, TimestampedProcessedRTCM> lastProcessedStore;

    private KeyValueStore<RsuStationIdKey, Timestamps> lastEventStore;

    /**
     * Stores a ProcessedRTCM message with its stream time and clock time
     * @param processedRTCM The message
     * @param timestamps The stream time and clock time
     */
    public record TimestampedProcessedRTCM(ProcessedRTCM processedRTCM, Timestamps timestamps) {}
    public static Serde<TimestampedProcessedRTCM> TimestampeddProcessedRTCMSerdes() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(TimestampedProcessedRTCM.class));
    }

    private final RtcmMessageCountProgressionParameters parameters;

    public RtcmMessageCountProgressionProcessor(RtcmMessageCountProgressionParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public void init(ProcessorContext<RsuStationIdKey, RtcmMessageCountProgressionEvent> context) {
        super.init(context);
        this.bufferStore = context.getStateStore(parameters.getProcessedRtcmStateStoreName());
        this.lastProcessedStore = context.getStateStore(parameters.getLatestRtcmStateStoreName());
        this.lastEventStore = context.getStateStore(parameters.getLatestEventStateStoreName());
        context.schedule(Duration.ofMillis(parameters.getCheckIntervalMs()), PunctuationType.WALL_CLOCK_TIME, this::punctuate);
    }

    @Override
    public void process(Record<RsuStationIdKey, ProcessedRTCM> record) {
        final RsuStationIdKey key = record.key();
        log.info("-----------------processing key {}", key);
        final ProcessedRTCM value = record.value();
        final long timestamp = record.timestamp();

        bufferStore.put(key, value, timestamp);
        log.info("added to bufferStore key: {}, timestamp: {}", key, timestamp);

        // Check for events here in case clock time doesn't advance
        checkForEvents(timestamp, key);

        var timestamps = new Timestamps(context().currentStreamTimeMs(), context().currentSystemTimeMs());
        var timestampedRtcm = new TimestampedProcessedRTCM(value, timestamps);
        log.info("Adding key {} value {} to lastProcessedStore", key, timestampedRtcm);
        lastProcessedStore.put(key, timestampedRtcm);
    }

    /**
     * Check for events for a particular key
     * @param currentTime The current time which may be stream time OR *relative* clock time depending on whether
     *                    called from the process method or from the punctuator
     * @param key The RSU Station ID key
     */
    private void checkForEvents(final long currentTime, final RsuStationIdKey key) {

        // Query within buffer time
        final Instant startTime = Instant.ofEpochMilli(currentTime - parameters.getBufferTimeMs());

        // Query up to the current time excluding grace period
        final Instant endTime = Instant.ofEpochMilli(currentTime - parameters.getBufferGracePeriodMs());

        log.info("query from {} to {}", startTime.toEpochMilli(), endTime.toEpochMilli());

        // Do the versioned store query
        var query = MultiVersionedKeyQuery.<RsuStationIdKey, ProcessedRTCM>withKey(key)
                .fromTime(startTime)
                .toTime(endTime)
                .withAscendingTimestamps();

        log.info("query {}", query);

        QueryResult<VersionedRecordIterator<ProcessedRTCM>> result
                = bufferStore.query(query, PositionBound.unbounded(),
                new QueryConfig(false));

        log.info("result: {}", result);


        if (result.isSuccess() && result.getResult() instanceof VersionedRecordIterator<ProcessedRTCM> iterator && iterator.hasNext()) {
            VersionedRecord<ProcessedRTCM> previousState = null;
            int recordCount = 0;
            while (iterator.hasNext()) {
                final VersionedRecord<ProcessedRTCM> state = iterator.next();
                final ProcessedRTCM thisState = state.value();
                log.info("====this state: {}", thisState);




                recordCount++;

                final long thisTimestamp = state.timestamp();
                final Instant thisTime = Instant.ofEpochMilli(thisTimestamp);

                // Enforce the upper query bound here to avoid sending within grace period if query toTime doesn't work as expected
                if (thisTime.isAfter(endTime)) {
                    log.info("Skipping item at {} after the grace period time {}", thisTime.toEpochMilli(), endTime.toEpochMilli());
                    continue;
                }

                if (previousState != null) {
                    final long previousTimestamp = previousState.timestamp();

                    final long timeDifference = thisTimestamp - previousTimestamp;
                    log.info("thisTimestamp: {}, previousTimestamp: {}, timeDifference: {}", previousTimestamp, previousTimestamp, timeDifference);
                    if (timeDifference < parameters.getBufferTimeMs()) {
                        DiffResult<ProcessedRTCM> diffResult = RtcmUtils.compare(previousState.value(), thisState);
                        final boolean valuesDiffer = diffResult.getNumberOfDiffs() > 0;
                        final int thisMsgCnt = thisState.getProperties().getMsgCnt();
                        final int previousMsgCnt = previousState.value().getProperties().getMsgCnt();
                        final boolean msgCntDiffers = thisMsgCnt != previousMsgCnt;
                        if ((valuesDiffer && !msgCntDiffers) // Values changed with same message count
                                || (!valuesDiffer & msgCntDiffers)) { // Values the same with different message count

                            // Check if the event was sent already
                            var lastEvent = lastEventStore.get(key);
                            if (lastEvent != null && lastEvent.streamTime() >= thisTimestamp) {
                                log.info("Already sent this event");
                            } else {
                                final var event = new RtcmMessageCountProgressionEvent();
                                event.setMessageCountA(previousMsgCnt);
                                event.setMessageCountB(thisMsgCnt);
                                event.setSource(thisState.getProperties().getOriginIp());
                                event.setTimestampA(previousTimestamp);
                                event.setTimestampB(thisTimestamp);
                                event.setStationId(thisState.getProperties().getStationId());
                                event.setChange(RtcmUtils.listDifferingFields(diffResult));
                                context().forward(new Record<>(key, event, state.timestamp()));
                                lastEventStore.put(key, new Timestamps(thisTimestamp, context().currentSystemTimeMs()));
                                log.info("forwarded event key {}, timestamp {}, event {}", key, state.timestamp(), event);
                            }
                        } else {
                            log.info("values don't differ, not forwarding event");
                        }
                    }
                } else {
                    log.info("previous state is null");
                }
                previousState = state;
                log.info("previous state set to {}", previousState);
            }
            log.info("record count: {}", recordCount);

        }






    }

    /**
     * Punctuator checks for events for all keys on relative clock time intervals in case messages are sparse and
     * stream time doesn't advance.
     * @param punctuateTime Clock time millis
     */
    private void punctuate(final long punctuateTime) {
        log.info("punctuate {}", punctuateTime);
        try (KeyValueIterator<RsuStationIdKey, TimestampedProcessedRTCM> iterator = lastProcessedStore.all()) {
            while (iterator.hasNext()) {
                final var keyValue = iterator.next();
                final RsuStationIdKey key = keyValue.key;
                final TimestampedProcessedRTCM value = keyValue.value;
                final long streamTime = value.timestamps().streamTime();
                final long clockTime = value.timestamps().clockTime();
                final long offset = value.timestamps().offset();

                // Offset the actual clock time to get the time that would have elapsed relative to stream time.
                // If messages are ingested in near real time, this offset will be small,
                // but if stored messages are replayed with their original timestamps the offset will be large.
                // This logic accounts for that large offset.
                final long offsetPunctuateTime = punctuateTime - offset;
                log.info("punctuateTime {} streamTime {} clockTime {} offset {} offsetPunctuateTime {}",
                        punctuateTime, streamTime, clockTime, offset, offsetPunctuateTime);

                checkForEvents(offsetPunctuateTime, key);
            }
        }
    }

}
