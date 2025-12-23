package us.dot.its.jpo.conflictmonitor.monitor.processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.DiffResult;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.MultiVersionedKeyQuery;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.apache.kafka.streams.state.VersionedRecordIterator;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression.RtcmMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.RtcmMessageCountProgressionEvent;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.TimestampExtractorForBroadcastRate;
import us.dot.its.jpo.conflictmonitor.monitor.utils.RtcmUtils;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;

import java.time.Instant;

@Slf4j
public class RtcmMessageCountProgressionProcessor
        extends ContextualProcessor<RsuStationIdKey, ProcessedRTCM, RsuStationIdKey, RtcmMessageCountProgressionEvent> {

    private VersionedKeyValueStore<RsuStationIdKey, ProcessedRTCM> bufferStore;
    private KeyValueStore<RsuStationIdKey, ProcessedRTCM> lastProcessedStore;
    private final RtcmMessageCountProgressionParameters parameters;

    public RtcmMessageCountProgressionProcessor(RtcmMessageCountProgressionParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public void init(ProcessorContext<RsuStationIdKey, RtcmMessageCountProgressionEvent> context) {
        super.init(context);
        this.bufferStore = context.getStateStore(parameters.getProcessedRtcmStateStoreName());
        this.lastProcessedStore = context.getStateStore(parameters.getLatestRtcmStateStoreName());
    }

    @Override
    public void process(Record<RsuStationIdKey, ProcessedRTCM> record) {
        final RsuStationIdKey key = record.key();
        log.info("-----------------processing key {}", key);
        final ProcessedRTCM value = record.value();
        final long timestamp = record.timestamp();
//        log.info("timestamp: {}", timestamp);
//        log.info("stream time: {}", context().currentStreamTimeMs());

//        var getLatest = bufferStore.get(key);
//        log.info("getLatest: {}", getLatest);
//        if (getLatest != null) {
//            log.info("getLatest timestap: {}", getLatest.timestamp());
//        }

        bufferStore.put(key, value, timestamp);
        log.info("added to bufferStore key: {}, timestamp: {}", key, timestamp);


        // Query buffer excluding grace period relative to stream time
        Instant excludeGracePeriod =
                Instant.ofEpochMilli(context().currentStreamTimeMs())
                        .minusMillis(parameters.getBufferGracePeriodMs());

        //log.info("query timestamp {}", excludeGracePeriod.toEpochMilli());

        final ProcessedRTCM lastProcessed = lastProcessedStore.get(key);
        log.info("last processed RTCM: {}", lastProcessed);

        final Long lastProcessedTimestamp = lastProcessed != null ? TimestampExtractorForBroadcastRate.extractTimestamp(lastProcessed) : null;

        Instant startTime;

        if (lastProcessedTimestamp != null && lastProcessedTimestamp > 0) {
            startTime = Instant.ofEpochMilli(lastProcessedTimestamp);
        } else {
            // There haven't been any transitions yet, use stream time minus buffer time
            startTime = Instant.ofEpochMilli(context().currentStreamTimeMs())
                    .minusMillis(parameters.getBufferTimeMs());
        }

        //log.info("startTime {}", startTime.toEpochMilli());

        // Ensure excludeGracePeriod is not earlier than startTime
        if (excludeGracePeriod.isBefore(startTime)) {
            excludeGracePeriod = startTime;
        }
        //log.info("query timestamp {}", excludeGracePeriod.toEpochMilli());

        // Add a small buffer to include the exact startTime record
        final Instant queryFrom = startTime.minusMillis(1);
        final Instant queryTo = excludeGracePeriod;
        log.info("query from {} to {}", queryFrom.toEpochMilli(), queryTo.toEpochMilli());

        // Do the versioned store query
        var query = MultiVersionedKeyQuery.<RsuStationIdKey, ProcessedRTCM>withKey(key)
                .fromTime(queryFrom)
                //.toTime(queryTo)
                .withAscendingTimestamps();

        log.info("query {}", query);

        QueryResult<VersionedRecordIterator<ProcessedRTCM>> result
                = bufferStore.query(query, PositionBound.unbounded(),
                new QueryConfig(false));

        log.info("result: {}", result);

        if (result.isSuccess() && result.getResult() instanceof VersionedRecordIterator<ProcessedRTCM> iterator && iterator.hasNext()) {
//            VersionedRecordIterator<ProcessedRTCM> iterator = result.getResult();
            ProcessedRTCM previousState = null;
            int recordCount = 0;
            while (iterator.hasNext()) {
                final VersionedRecord<ProcessedRTCM> state = iterator.next();
                final ProcessedRTCM thisState = state.value();
                log.info("====this state: {}", thisState);
                recordCount++;

                final long thisTimestamp = TimestampExtractorForBroadcastRate.extractTimestamp(thisState);
                final Instant thisTime = Instant.ofEpochMilli(thisTimestamp);

                log.info("this time: {}, last processed time: {}", thisTimestamp, lastProcessedTimestamp);

                // Skip records older than the last processed state
                if (lastProcessed != null) {
                    final Instant lastProcessedTime = Instant.ofEpochMilli(lastProcessedTimestamp);
                    if (thisTime.isBefore(lastProcessedTime)) {
                        log.info("Skipping item at {} earlier than last processed time {}", thisTime, lastProcessedTime);
                        continue;
                    }
                }

                if (previousState != null) {
                    final long previousTimestamp = TimestampExtractorForBroadcastRate.extractTimestamp(previousState);

                    final long timeDifference = thisTimestamp - previousTimestamp;
                    log.info("thisTimestamp: {}, previousTimestamp: {}, timeDifference: {}", previousTimestamp, previousTimestamp, timeDifference);
                    if (timeDifference < parameters.getBufferTimeMs()) {
                        DiffResult<ProcessedRTCM> diffResult = RtcmUtils.compare(previousState, thisState);
                        final boolean valuesDiffer = diffResult.getNumberOfDiffs() > 0;
                        final int thisMsgCnt = thisState.getProperties().getMsgCnt();
                        final int previousMsgCnt = previousState.getProperties().getMsgCnt();
                        final boolean msgCntDiffers = thisMsgCnt != previousMsgCnt;
                        if ((valuesDiffer && !msgCntDiffers) // Values changed with same message count
                                || (!valuesDiffer & msgCntDiffers)) { // Values the same with different message count
                            final var event = new RtcmMessageCountProgressionEvent();
                            event.setMessageCountA(previousMsgCnt);
                            event.setMessageCountB(thisMsgCnt);
                            event.setSource(thisState.getProperties().getOriginIp());
                            event.setTimestampA(previousTimestamp);
                            event.setTimestampB(thisTimestamp);
                            event.setStationId(thisState.getProperties().getStationId());
                            event.setChange(RtcmUtils.listDifferingFields(diffResult));
                            context().forward(new Record<>(key, event, state.timestamp()));
                            log.info("forwarded event key {}, timestamp {}", key, state.timestamp());
                        }
                    }
                } else {
                    log.info("previous state is null");
                }
                previousState = thisState;
                log.info("previous state set to {}", previousState);
            }
            log.info("record count: {}", recordCount);
            if (recordCount > 1) {
                log.info("Adding key {} to lastProcessedStore", key);
                lastProcessedStore.put(key, previousState);
            }
        } else {
            log.info("Adding key {} value {} to lastProcessedStore", key, value);
            lastProcessedStore.put(key, value);
        }


    }


}
