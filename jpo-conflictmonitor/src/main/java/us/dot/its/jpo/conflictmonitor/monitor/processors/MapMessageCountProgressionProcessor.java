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

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.map_message_count_progression.MapMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.MapMessageCountProgressionEvent;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.ProcessedValidationMessage;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.LineString;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;
import us.dot.its.jpo.ode.model.OdeMessageFrameMetadata;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Slf4j
public class MapMessageCountProgressionProcessor extends ContextualProcessor<RsuIntersectionKey, ProcessedMap<LineString>, RsuIntersectionKey, MapMessageCountProgressionEvent> {

    private VersionedKeyValueStore<RsuIntersectionKey, ProcessedMap<LineString>> stateStore;
    private KeyValueStore<RsuIntersectionKey, ProcessedMap<LineString>> lastProcessedStateStore;
    private final MapMessageCountProgressionParameters parameters;

    public MapMessageCountProgressionProcessor(MapMessageCountProgressionParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public void init(ProcessorContext<RsuIntersectionKey, MapMessageCountProgressionEvent> context) {
        super.init(context);
        stateStore = context.getStateStore(parameters.getProcessedMapStateStoreName());
        lastProcessedStateStore = context.getStateStore(parameters.getLatestMapStateStoreName());
    }

    @Override
    public void process(Record<RsuIntersectionKey, ProcessedMap<LineString>> record) {
        log.debug("Processing record with key {} at timestamp {}, stream time: {}", record.key(),
                Instant.ofEpochMilli(record.timestamp()), Instant.ofEpochMilli(context().currentStreamTimeMs()));
        RsuIntersectionKey key = record.key();
        ProcessedMap<LineString> value = record.value();
        long timestamp = record.timestamp();

        // Insert new record into the buffer
        stateStore.put(key, value, timestamp);
        log.debug("Add to state store at timestamp: {}", Instant.ofEpochMilli(timestamp));

        // Query the buffer, excluding the grace period relative to stream time "now".
        Instant excludeGracePeriod =
                Instant.ofEpochMilli(context().currentStreamTimeMs())
                        .minusMillis(parameters.getBufferGracePeriodMs());

        ProcessedMap<LineString> lastProcessedMap = lastProcessedStateStore.get(key);
        Instant startTime;
        if (lastProcessedMap != null) {
            startTime = lastProcessedMap.getProperties().getOdeReceivedAt().toInstant();
        } else {
            // No transitions yet, base start time on time window
            startTime = excludeGracePeriod.minusMillis(parameters.getBufferTimeMs());
            // Populate last processed
            lastProcessedStateStore.put(key, value);
        }
        log.debug("startTime: {}", startTime);

        var query = MultiVersionedKeyQuery.<RsuIntersectionKey, ProcessedMap<LineString>>withKey(record.key())
                .fromTime(startTime.minusMillis(1)) // Add a small buffer to include the exact startTime record
                .toTime(excludeGracePeriod)
                .withAscendingTimestamps();

        log.debug("query between {} and {}", startTime.minusMillis(1), excludeGracePeriod);

        QueryResult<VersionedRecordIterator<ProcessedMap<LineString>>> result = stateStore.query(query,
                PositionBound.unbounded(),
                new QueryConfig(false));

        log.debug("query success: {}", result.isSuccess());

        if (result.isSuccess()) {
            try (VersionedRecordIterator<ProcessedMap<LineString>> iterator = result.getResult()) {
                ProcessedMap<LineString> previousState = null;
                int recordCount = 0;

                while (iterator.hasNext()) {
                    final VersionedRecord<ProcessedMap<LineString>> state = iterator.next();
                    final ProcessedMap<LineString> thisState = state.value();
                    recordCount++;
                    log.debug("record count: {}", recordCount);

                    // Skip records older than the last processed state
                    ZonedDateTime thisReceivedAt = thisState.getProperties().getOdeReceivedAt();
                    if (lastProcessedMap != null && thisReceivedAt.isBefore(lastProcessedMap.getProperties().getOdeReceivedAt())) {
                        log.debug("Skipping record with ODE received at {} older than last processed at {}",
                                thisState.getProperties().getOdeReceivedAt(), lastProcessedMap.getProperties().getOdeReceivedAt());
                        continue;
                    }

                    // Check if we are within the buffer and grace period
                    Instant thisReceivedAtInstant = thisReceivedAt.toInstant();
                    Instant streamTimeInstant = Instant.ofEpochMilli(context().currentStreamTimeMs());
                    Duration durationSinceFirst = Duration.between(thisReceivedAtInstant, streamTimeInstant);
                    Duration bufferAndGraceDuration = Duration.ofMillis((long)parameters.getBufferTimeMs() +
                            parameters.getBufferGracePeriodMs());
                    if (recordCount == 1 && durationSinceFirst.compareTo(bufferAndGraceDuration) < 0) {
                        // Don't finish processing yet, buffer + grace hasn't elapsed relative to the first record
                        log.debug("not processing yet, buffer + grace hasn't elapsed relative to the first record: {} < {}", durationSinceFirst, bufferAndGraceDuration);
                        break;
                    }

                    log.debug("Previous state exists: {}", previousState != null);

                    if (previousState != null) {
                        long timeDifference = thisState.getProperties().getOdeReceivedAt().toInstant().toEpochMilli()
                                - previousState.getProperties().getOdeReceivedAt().toInstant().toEpochMilli();
                        log.debug("timeDifference: {}, bufferTime: {}", timeDifference, parameters.getBufferTimeMs());

                        // Don't compare if the time difference is not in the ballpark of what it should be for MAPs
                        // (nominally 1Hz), otherwise there may be missing MAPs so the increment would not be known.
                        final long minDifferenceMs = 500L;
                        final long maxDifferenceMs = 1500L;
                        if (timeDifference >= minDifferenceMs && timeDifference <= maxDifferenceMs) {
                            int previousRevision = previousState.getProperties().getRevision();
                            int currentRevision = thisState.getProperties().getRevision();
                            int previousMsgIssueRevision = previousState.getProperties().getMsgIssueRevision();
                            int currentMsgIssueRevision = thisState.getProperties().getMsgIssueRevision();

                            boolean isEqual = testEquality(previousState, thisState);
                            boolean contentsChanged = !isEqual;

                            int revisionChange = Math.floorMod(currentRevision - previousRevision, 128);
                            boolean revisionChanged = revisionChange != 0;
                            boolean revisionIncrementedByOne = revisionChange == 1;

                            int msgIssueRevisionChange = Math.floorMod(currentMsgIssueRevision - previousMsgIssueRevision, 128);
                            boolean msgIssueRevisionChanged = msgIssueRevisionChange != 0;
                            boolean msgIssueRevisionIncrementedByOne = msgIssueRevisionChange == 1;

                            boolean anyRevisionChanged = revisionChanged || msgIssueRevisionChanged;
                            // A counter that changed at all must have changed by exactly +1 (mod 128);
                            // anything else (skipped a count, went backwards, etc.) is anomalous
                            // regardless of the other counter.
                            boolean anyRevisionChangedByWrongAmount =
                                    (revisionChanged && !revisionIncrementedByOne)
                                            || (msgIssueRevisionChanged && !msgIssueRevisionIncrementedByOne);

                            log.debug("revisionChanged: {}, msgIssueRevisionChanged: {}, anyRevisionChanged: {}, " +
                                            "anyRevisionChangedByWrongAmount: {}, contentsChanged: {}",
                                    revisionChanged, msgIssueRevisionChanged, anyRevisionChanged,
                                    anyRevisionChangedByWrongAmount, contentsChanged);

                            if ((anyRevisionChanged && !contentsChanged)
                                  || (!anyRevisionChanged && contentsChanged)
                                  || anyRevisionChangedByWrongAmount) {
                                // Revision changed, but contents didn't change,
                                // or contents changed, but revision didn't,
                                // or a revision changed by other than +1.
                                // Issue an event in this case, this is a problem.
                                log.debug("producing event");
                                boolean reportRevision;
                                if (revisionChanged && !revisionIncrementedByOne) {
                                    reportRevision = true;
                                } else if (msgIssueRevisionChanged && !msgIssueRevisionIncrementedByOne) {
                                    reportRevision = false;
                                } else {
                                    reportRevision = revisionChanged;
                                }
                                MapMessageCountProgressionEvent event = createEvent(previousState, thisState, reportRevision);
                                context().forward(new Record<>(key, event, state.timestamp()));
                            } else {
                                log.debug("no event produced");
                            }
                        } else {
                            log.warn("MAP time difference {} ms is out of the normal range of {} - {} ms, " +
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

    private boolean testEquality(ProcessedMap<LineString> map1, ProcessedMap<LineString> map2) {
        if (map1 == null && map2 == null) {
            return true;
        }
        if (map1 == null) {
            return false;
        }
        if (map2 == null) {
            return false;
        }

        // Exclude revisions, timestamps and other metadata that's not part of the original MAP
        final MetadataProperties metadata1 = MetadataProperties.fromProcessedMap(map1);
        final MetadataProperties metadata2 = MetadataProperties.fromProcessedMap(map2);
        boolean equality;
        // synchronize during mutate and restore for thread safety
        synchronized (this) {
            try {
                nullMetadataProperties(map1);
                nullMetadataProperties(map2);
                equality = map1.equals(map2);
            } finally {
                restoreMetadataProperties(map1, metadata1);
                restoreMetadataProperties(map2, metadata2);
            }
        }
        return equality;
    }

    private record MetadataProperties(ZonedDateTime odeReceivedAt, ZonedDateTime timeStamp, int revision,
                                      int msgIssueRevision, String originIp, String asn1,
                                      List<ProcessedValidationMessage> validationMessages,
                                      OdeMessageFrameMetadata.Source source) {
        public static MetadataProperties fromProcessedMap(ProcessedMap<LineString> map) {
            if (map == null) {
                return new MetadataProperties(null, null, 0, 0, null, null, null, null);
            }
            return new MetadataProperties(
                    map.getProperties().getOdeReceivedAt(),
                    map.getProperties().getTimeStamp(),
                    map.getProperties().getRevision(),
                    map.getProperties().getMsgIssueRevision(),
                    map.getProperties().getOriginIp(),
                    map.getProperties().getAsn1(),
                    map.getProperties().getValidationMessages(),
                    map.getProperties().getMapSource());
        }
    }

    private void nullMetadataProperties(ProcessedMap<LineString> map) {
        map.getProperties().setOdeReceivedAt(null);
        map.getProperties().setTimeStamp(null);
        map.getProperties().setRevision(0);
        map.getProperties().setMsgIssueRevision(0);
        map.getProperties().setOriginIp(null);
        map.getProperties().setAsn1(null);
        map.getProperties().setValidationMessages(null);
        map.getProperties().setMapSource(null);
    }

    private void restoreMetadataProperties(ProcessedMap<LineString> map, MetadataProperties metadata) {
        map.getProperties().setOdeReceivedAt(metadata.odeReceivedAt);
        map.getProperties().setTimeStamp(metadata.timeStamp);
        map.getProperties().setRevision(metadata.revision);
        map.getProperties().setMsgIssueRevision(metadata.msgIssueRevision);
        map.getProperties().setOriginIp(metadata.originIp);
        map.getProperties().setAsn1(metadata.asn1);
        map.getProperties().setValidationMessages(metadata.validationMessages);
        map.getProperties().setMapSource(metadata.source);
    }

    private MapMessageCountProgressionEvent createEvent(ProcessedMap<LineString> previousState, ProcessedMap<LineString> thisState, boolean reportRevision) {
        MapMessageCountProgressionEvent event = new MapMessageCountProgressionEvent();
        event.setMessageType("MAP");

        if (reportRevision) {
            event.setMessageCountA(previousState.getProperties().getRevision());
            event.setMessageCountB(thisState.getProperties().getRevision());
        } else {
            event.setMessageCountA(previousState.getProperties().getMsgIssueRevision());
            event.setMessageCountB(thisState.getProperties().getMsgIssueRevision());
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        event.setTimestampA(previousState.getProperties().getOdeReceivedAt().format(formatter));
        event.setTimestampB(thisState.getProperties().getOdeReceivedAt().format(formatter));
        if (thisState.getProperties().getRegion() != null) {
            event.setRoadRegulatorID(thisState.getProperties().getRegion());
        } else {
            event.setRoadRegulatorID(-1);
        }
        if (thisState.getProperties().getIntersectionId() != null) {
            event.setIntersectionID(thisState.getProperties().getIntersectionId());
        } else {
            event.setIntersectionID(-1);
        }
        event.setSource(thisState.getProperties().getOriginIp());
        return event;
    }

    @Override
    public void close() {
        super.close();
    }
}