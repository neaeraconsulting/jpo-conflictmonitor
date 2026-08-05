package us.dot.its.jpo.conflictmonitor.monitor.processors;

import lombok.extern.slf4j.Slf4j;
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

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.bsm_message_count_progression.BsmMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.bsm.BsmTimestampExtractor;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.BsmMessageCountProgressionEvent;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuLogKey;
import us.dot.its.jpo.geojsonconverter.pojos.ProcessedValidationMessage;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.bsm.BsmProperties;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.bsm.ProcessedBsm;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

// TODO Use RsuLogKey
@Slf4j
public class BsmMessageCountProgressionProcessor<Point> extends ContextualProcessor<RsuLogKey, ProcessedBsm<Point>, RsuLogKey, BsmMessageCountProgressionEvent> {

    private VersionedKeyValueStore<RsuLogKey, ProcessedBsm<Point>> stateStore;
    private KeyValueStore<RsuLogKey, ProcessedBsm<Point>> lastProcessedStateStore;
    private final BsmMessageCountProgressionParameters parameters;

    public BsmMessageCountProgressionProcessor(BsmMessageCountProgressionParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public void init(ProcessorContext<RsuLogKey, BsmMessageCountProgressionEvent> context) {
        super.init(context);
        stateStore = context.getStateStore(parameters.getProcessedBsmStateStoreName());
        lastProcessedStateStore = context.getStateStore(parameters.getLatestBsmStateStoreName());
    }

    @Override
    public void process(Record<RsuLogKey, ProcessedBsm<Point>> record) {
        RsuLogKey key = record.key();
        ProcessedBsm<Point> value = record.value();
        long timestamp = record.timestamp();

        // Insert new record into the buffer
        stateStore.put(key, value, timestamp);

        // Query the buffer, excluding the grace period relative to stream time "now".
        Instant excludeGracePeriod =
                Instant.ofEpochMilli(context().currentStreamTimeMs())
                        .minusMillis(parameters.getBufferGracePeriodMs());

        ProcessedBsm<Point> lastProcessedBsm = lastProcessedStateStore.get(key);
        Instant startTime;
        // BsmTimestampExtractor.getBsmTimestamp(...) is the timestamp actually used to place a record in the
        // versioned store (it's what the topology's TimestampExtractor computes), which is not the same as
        // properties.getTimeStamp(). Use it consistently here instead of the raw field.
        final long lastProcessedTimestampMs;
        if (lastProcessedBsm != null) {
            lastProcessedTimestampMs = BsmTimestampExtractor.getBsmTimestamp(lastProcessedBsm);
            startTime = Instant.ofEpochMilli(lastProcessedTimestampMs);
        } else {
            // No transitions yet, base start time on time window
            startTime = excludeGracePeriod.minusMillis(parameters.getBufferTimeMs());
            // Populate last processed for the first time
            lastProcessedStateStore.put(key, value);
            lastProcessedTimestampMs = -1;
        }

        var query = MultiVersionedKeyQuery.<RsuLogKey, ProcessedBsm<Point>>withKey(record.key())
            .fromTime(startTime.minusMillis(1)) // Add a small buffer to include the exact startTime record
            .toTime(excludeGracePeriod)
            .withAscendingTimestamps();

        QueryResult<VersionedRecordIterator<ProcessedBsm<Point>>> result = stateStore.query(query,
                PositionBound.unbounded(),
                new QueryConfig(false));

        if (result.isSuccess()) {
            try (VersionedRecordIterator<ProcessedBsm<Point>> iterator = result.getResult()) {
                ProcessedBsm<Point> previousState = null;
                long previousTimestampMs = -1;
                int recordCount = 0;

                while (iterator.hasNext()) {
                    final VersionedRecord<ProcessedBsm<Point>> state = iterator.next();
                    final ProcessedBsm<Point> thisState = state.value();
                    final long thisTimestampMs = state.timestamp();
                    recordCount++;

                    // Skip records older than the last processed state
                    if (lastProcessedBsm != null && thisTimestampMs < lastProcessedTimestampMs) {
                        log.debug("Skipping record with timestamp {} older than last processed at {}",
                                thisTimestampMs, lastProcessedTimestampMs);
                        continue;
                    }

                    // Check if we are within the buffer and grace period
                    Instant thisTimestampInstant = Instant.ofEpochMilli(thisTimestampMs);
                    Instant streamTimeInstant = Instant.ofEpochMilli(context().currentStreamTimeMs());
                    Duration durationSinceFirst = Duration.between(thisTimestampInstant, streamTimeInstant);
                    Duration bufferAndGraceDuration = Duration.ofMillis(
                            (long) parameters.getBufferTimeMs() + parameters.getBufferGracePeriodMs());
                    if (recordCount == 1 && durationSinceFirst.compareTo(bufferAndGraceDuration) < 0) {
                        // Don't finish processing yet. Buffer + grace hasn't elapsed relative to the first record
                        break;
                    }

                    if (previousState != null) {
                        final long timeDifference = thisTimestampMs - previousTimestampMs;

                        // Don't compare if the time difference is not in the ballpark of what it should be for BSMs
                        // otherwise there may be missing BSMs so the increment would not be known.
                        final long minDifferenceMs = 60L;
                        final long maxDifferenceMs = 150L;
                        if (timeDifference >= minDifferenceMs && timeDifference <= maxDifferenceMs) {
                            BsmProperties previousProperties = previousState.getProperties();
                            BsmProperties currentProperties = thisState.getProperties();
                            int previousMessageCount = previousProperties.getMsgCnt();
                            int currentMessageCount = currentProperties.getMsgCnt();

                            boolean contentsChanged = !testEquality(previousState, thisState);
                            boolean countChanged = previousMessageCount != currentMessageCount;
                            int countChange =
                                    ((currentMessageCount + 1) % 128) - ((previousMessageCount + 1) % 128);
                            boolean countIncremented = countChange == 1;

                            if ((countChanged && !contentsChanged)
                                 || (!countIncremented && contentsChanged)) {
                                // Count changed but contents did not,
                                // or contents changed but count did not increment by 1.
                                // Issue an event, this is a problem
                                log.debug("producing event");
                                BsmMessageCountProgressionEvent event =
                                        createEvent(previousState, previousTimestampMs, thisState, thisTimestampMs);
                                context().forward(new Record<>(key, event, state.timestamp()));
                            } else {
                                log.debug("no event produced");
                            }
                        } else {
                            log.warn("BSM time difference {} ms is out of the normal range of {} - {} ms, " +
                                            "not checking message count increment",
                                    timeDifference, minDifferenceMs, maxDifferenceMs);
                        }
                    }
                    previousState = thisState;
                    previousTimestampMs = thisTimestampMs;
                }
                if (recordCount > 1) {
                    // Update last processed state
                    lastProcessedStateStore.put(key, previousState);
                }
            }
        }
    }

    private boolean testEquality(ProcessedBsm<Point> bsm1, ProcessedBsm<Point> bsm2) {
        if (bsm1 == null && bsm2 == null) {
            return true;
        }
        if (bsm1 == null || bsm2 == null) {
            return false;
        }

        // Exclude message counts, timestamps and other metadata that's not part of the original BSM
        final MetadataProperties metadata1 = MetadataProperties.fromProcessedBsm(bsm1);
        final MetadataProperties metadata2 = MetadataProperties.fromProcessedBsm(bsm2);
        boolean equality;
        // synchronize during mutate and restore for thread safety
        synchronized (this) {
            try {
                nullMetadataProperties(bsm1);
                nullMetadataProperties(bsm2);
                equality = bsm1.equals(bsm2);
            } finally {
                restoreMetadataProperties(bsm1, metadata1);
                restoreMetadataProperties(bsm2, metadata2);
            }
        }
        return equality;
    }

    private record MetadataProperties(String odeReceivedAt, ZonedDateTime timeStamp, Integer secMark, int msgCnt,
                                       String originIp, String asn1, String logName,
                                       List<ProcessedValidationMessage> validationMessages) {
        public static MetadataProperties fromProcessedBsm(ProcessedBsm<?> bsm) {
            BsmProperties properties = bsm.getProperties();
            return new MetadataProperties(
                properties.getOdeReceivedAt(),
                properties.getTimeStamp(),
                properties.getSecMark(),
                properties.getMsgCnt(),
                properties.getOriginIp(),
                properties.getAsn1(),
                properties.getLogName(),
                properties.getValidationMessages()
            );
        }
    }

    private void nullMetadataProperties(ProcessedBsm<Point> bsm) {
        BsmProperties properties = bsm.getProperties();
        properties.setOdeReceivedAt(null);
        properties.setTimeStamp(null);
        properties.setSecMark(null);
        properties.setMsgCnt(0);
        properties.setOriginIp(null);
        properties.setAsn1(null);
        properties.setLogName(null);
        properties.setValidationMessages(null);
    }

    private void restoreMetadataProperties(ProcessedBsm<Point> bsm, MetadataProperties metadata) {
        BsmProperties properties = bsm.getProperties();
        properties.setOdeReceivedAt(metadata.odeReceivedAt);
        properties.setTimeStamp(metadata.timeStamp);
        properties.setSecMark(metadata.secMark);
        properties.setMsgCnt(metadata.msgCnt);
        properties.setOriginIp(metadata.originIp);
        properties.setAsn1(metadata.asn1);
        properties.setLogName(metadata.logName);
        properties.setValidationMessages(metadata.validationMessages);
    }

    private BsmMessageCountProgressionEvent createEvent(ProcessedBsm<Point> previousState, long previousTimestampMs,
                                                          ProcessedBsm<Point> thisState, long thisTimestampMs) {
        BsmProperties previousProperties = previousState.getProperties();
        BsmProperties currentProperties = thisState.getProperties();

        BsmMessageCountProgressionEvent event = new BsmMessageCountProgressionEvent();
        event.setMessageType("BSM");
        event.setMessageCountA(previousProperties.getMsgCnt());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        event.setTimestampA(Instant.ofEpochMilli(previousTimestampMs).atZone(ZoneOffset.UTC).format(formatter));
        event.setMessageCountB(currentProperties.getMsgCnt());
        event.setTimestampB(Instant.ofEpochMilli(thisTimestampMs).atZone(ZoneOffset.UTC).format(formatter));
        if (thisState.getProperties().getId() != null) {
            event.setVehicleId(thisState.getProperties().getId());
        } else if (thisState.getId() != null) {
            event.setVehicleId(thisState.getId().toString());
        }
        event.setSource(thisState.getProperties().getOriginIp());

        return event;
    }

    @Override
    public void close() {
        super.close();
    }
}
