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

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.spat_message_count_progression.SpatMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.SpatMessageCountProgressionEvent;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.ProcessedValidationMessage;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Slf4j
public class SpatMessageCountProgressionProcessor extends ContextualProcessor<RsuIntersectionKey, ProcessedSpat, RsuIntersectionKey, SpatMessageCountProgressionEvent> {

    private VersionedKeyValueStore<RsuIntersectionKey, ProcessedSpat> stateStore;
    private KeyValueStore<RsuIntersectionKey, ProcessedSpat> lastProcessedStateStore;
    private final SpatMessageCountProgressionParameters parameters;

    public SpatMessageCountProgressionProcessor(SpatMessageCountProgressionParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public void init(ProcessorContext<RsuIntersectionKey, SpatMessageCountProgressionEvent> context) {
        super.init(context);
        stateStore = context.getStateStore(parameters.getProcessedSpatStateStoreName());
        lastProcessedStateStore = context.getStateStore(parameters.getLatestSpatStateStoreName());
    }

    @Override
    public void process(Record<RsuIntersectionKey, ProcessedSpat> record) {
        RsuIntersectionKey key = record.key();
        ProcessedSpat value = record.value();
        long timestamp = record.timestamp();
    
        // Insert new record into the buffer
        stateStore.put(key, value, timestamp);
    
        // Query the buffer, excluding the grace period relative to stream time "now".
        Instant excludeGracePeriod =
                Instant.ofEpochMilli(context().currentStreamTimeMs())
                        .minusMillis(parameters.getBufferGracePeriodMs());
    
        ProcessedSpat lastProcessedSpat = lastProcessedStateStore.get(key);
        Instant startTime;
        if (lastProcessedSpat != null) {
            startTime = lastProcessedSpat.getUtcTimeStamp().toInstant();
        } else {
            // No transitions yet, base start time on time window
            startTime = excludeGracePeriod.minusMillis(parameters.getBufferTimeMs());
            // Populate last processed for the first time
            lastProcessedStateStore.put(key, value);
        }
        
        var query = MultiVersionedKeyQuery.<RsuIntersectionKey, ProcessedSpat>withKey(record.key())
            .fromTime(startTime.minusMillis(1)) // Add a small buffer to include the exact startTime record
            .toTime(excludeGracePeriod)
            .withAscendingTimestamps();

        QueryResult<VersionedRecordIterator<ProcessedSpat>> result = stateStore.query(query,
                PositionBound.unbounded(),
                new QueryConfig(false));

        if (result.isSuccess()) {
            try (VersionedRecordIterator<ProcessedSpat> iterator = result.getResult()) {
                ProcessedSpat previousState = null;
                int recordCount = 0;

                while (iterator.hasNext()) {
                    final VersionedRecord<ProcessedSpat> state = iterator.next();
                    final ProcessedSpat thisState = state.value();
                    recordCount++;

                    // Skip records older than the last processed state
                    ZonedDateTime thisTimestamp = thisState.getUtcTimeStamp();
                    if (lastProcessedSpat != null && thisTimestamp.isBefore(lastProcessedSpat.getUtcTimeStamp())) {
                        log.debug("Skipping record with timestamp {} older than last processed at {}",
                                thisTimestamp, lastProcessedSpat.getUtcTimeStamp());
                        continue;
                    }

                    // Check if we are within the buffer and grace period
                    Instant thisTimestampInstant = thisTimestamp.toInstant();
                    Instant streamTimeInstant = Instant.ofEpochMilli(context().currentStreamTimeMs());
                    Duration durationSinceFirst = Duration.between(thisTimestampInstant, streamTimeInstant);
                    Duration bufferAndGraceDuration = Duration.ofMillis(
                            (long)parameters.getBufferTimeMs() + parameters.getBufferGracePeriodMs());
                    if (recordCount == 1 && durationSinceFirst.compareTo(bufferAndGraceDuration) < 0) {
                        // Don't finish processing yet. Buffer + grace hasn't elapsed relative to the first record
                        break;
                    }

                    if (previousState != null) {
                        final long timeDifference = thisState.getUtcTimeStamp().toInstant().toEpochMilli()
                                - previousState.getUtcTimeStamp().toInstant().toEpochMilli();

                        // Don't compare if the time difference is not in the ballpark of what it should be for SPATs
                        // otherwise there may be missing SPATs so the increment would not be known.
                        final long minDifferenceMs = 60L;
                        final long maxDifferenceMs = 150L;
                        if (timeDifference >= minDifferenceMs && timeDifference <= maxDifferenceMs) {
                            int previousRevision = previousState.getRevision();
                            int currentRevision = thisState.getRevision();

                            boolean isEqual = testEquality(previousState, thisState);
                            boolean contentsChanged = !isEqual;
                            boolean revisionChanged = previousRevision != currentRevision;
                            int revisionChange =
                                    ((currentRevision + 1) % 128) - ((previousRevision + 1) % 128);
                            boolean revisionIncremented = revisionChange == 1;

                            if ((revisionChanged && !contentsChanged)
                                 || (!revisionIncremented && contentsChanged)) {
                                // Revision changed but contents did not,
                                // or contents changed but revision did not increment by 1.
                                // Issue an event, this is a problem
                                log.debug("producing event");
                                SpatMessageCountProgressionEvent event = createEvent(previousState, thisState);
                                context().forward(new Record<>(key, event, state.timestamp()));
                            } else {
                                log.debug("no event produced");
                            }
                        } else {
                            log.warn("SPAT time difference {} ms is out of the normal range of {} - {} ms, " +
                                            "not checking message count increment",
                                    timeDifference, minDifferenceMs, maxDifferenceMs);
                        }
                    }
                    previousState = thisState;
                }
                if (recordCount > 1) {
                    // Update last processed state
                    lastProcessedStateStore.put(key, previousState);
                }
            }
        }
    }

    private boolean testEquality(ProcessedSpat spat1, ProcessedSpat spat2) {
        if (spat1 == null && spat2 == null) {
            return true;
        }
        if (spat1 == null || spat2 == null) {
            return false;
        }

        // Exclude revisions, timestamps and other metadata that's not part of the original SPAT
        final MetadataProperties metadata1 = MetadataProperties.fromProcessedSpat(spat1);
        final MetadataProperties metadata2 = MetadataProperties.fromProcessedSpat(spat2);
        boolean equality;
        // synchronize during mutate and restore for thread safety
        synchronized (this) {
            try {
                nullMetadataProperties(spat1);
                nullMetadataProperties(spat2);
                equality = spat1.equals(spat2);
            } finally {
                restoreMetadataProperties(spat1, metadata1);
                restoreMetadataProperties(spat2, metadata2);
            }
        }
        return equality;
    }

    private record MetadataProperties(String odeReceivedAt, ZonedDateTime utcTimeStamp, int revision,
                                      String originIp, String asn1,
                                      List<ProcessedValidationMessage> validationMessages) {
        public static MetadataProperties fromProcessedSpat(ProcessedSpat spat) {
            return new MetadataProperties(
                spat.getOdeReceivedAt(),
                spat.getUtcTimeStamp(),
                spat.getRevision(),
                spat.getOriginIp(),
                spat.getAsn1(),
                spat.getValidationMessages()
            );
        }
    }

    private void nullMetadataProperties(ProcessedSpat spat) {
        spat.setOdeReceivedAt(null);
        spat.setUtcTimeStamp(null);
        spat.setRevision(0);
        spat.setOriginIp(null);
        spat.setAsn1(null);
        spat.setValidationMessages(null);
    }

    private void restoreMetadataProperties(ProcessedSpat spat, MetadataProperties metadata) {
        spat.setOdeReceivedAt(metadata.odeReceivedAt);
        spat.setUtcTimeStamp(metadata.utcTimeStamp);
        spat.setRevision(metadata.revision);
        spat.setOriginIp(metadata.originIp);
        spat.setAsn1(metadata.asn1);
        spat.setValidationMessages(metadata.validationMessages);
    }

    private SpatMessageCountProgressionEvent createEvent(ProcessedSpat previousState, ProcessedSpat thisState) {
        SpatMessageCountProgressionEvent event = new SpatMessageCountProgressionEvent();
        event.setMessageType("SPaT");
        event.setMessageCountA(previousState.getRevision());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        event.setTimestampA(previousState.getUtcTimeStamp().format(formatter));
        event.setMessageCountB(thisState.getRevision());
        event.setTimestampB(thisState.getUtcTimeStamp().format(formatter));
        if (thisState.getIntersectionId() != null) {
            event.setIntersectionID(thisState.getIntersectionId());
        } else {
            event.setIntersectionID(-1);
        }
        if (thisState.getRegion() != null) {
            event.setRoadRegulatorID(thisState.getRegion());
        } else {
            event.setRoadRegulatorID(-1);
        }
        event.setSource(thisState.getOriginIp());
        return event;
    }

    @Override
    public void close() {
        super.close();
    }
}