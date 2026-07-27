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
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.event_state_progression.EventStateProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.event_state_progression.EventStateProgressionState;
import us.dot.its.jpo.conflictmonitor.monitor.models.event_state_progression.RsuIntersectionSignalGroupKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.event_state_progression.SpatMovementState;
import us.dot.its.jpo.conflictmonitor.monitor.models.event_state_progression.SpatMovementStateTransition;

import java.time.Instant;
import java.util.Objects;

@Slf4j
public class EventStateProgressionProcessor
        extends ContextualProcessor<RsuIntersectionSignalGroupKey, SpatMovementState, RsuIntersectionSignalGroupKey, SpatMovementStateTransition> {

    VersionedKeyValueStore<RsuIntersectionSignalGroupKey, EventStateProgressionState> stateStore;
    KeyValueStore<RsuIntersectionSignalGroupKey, Long> latestTransitionStore;
    /** Last observed phase per signal group for unchanged-phase fast path. */
    KeyValueStore<RsuIntersectionSignalGroupKey, EventStateProgressionState> latestPhaseStore;

    final EventStateProgressionParameters parameters;

    public EventStateProgressionProcessor(EventStateProgressionParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public void init(ProcessorContext<RsuIntersectionSignalGroupKey, SpatMovementStateTransition> context) {
        super.init(context);
        stateStore = context.getStateStore(parameters.getMovementStateStoreName());
        latestTransitionStore = context.getStateStore(parameters.getLatestTransitionStoreName());
        latestPhaseStore = context.getStateStore(parameters.getLatestPhaseStoreName());
    }

    @Override
    public void process(Record<RsuIntersectionSignalGroupKey, SpatMovementState> record) {
        SpatMovementState value = record.value();
        if (value == null) {
            return;
        }

        if (parameters.isDebug()) {
            log.trace("Received record: timestamp {}, signal group {}, phase {}", record.timestamp(), record.key().getSignalGroup(),
                    value.getPhaseState());
        }

        EventStateProgressionState current = EventStateProgressionState.from(value);
        EventStateProgressionState lastPhase = latestPhaseStore.get(record.key());
        boolean phaseUnchanged = lastPhase != null
                && Objects.equals(lastPhase.getPhaseState(), current.getPhaseState());

        if (phaseUnchanged) {
            // Skip versioned put when phase is stable. Keep querying until stream time advances
            // past lastPut + grace so the put enters the grace-excluded query window at least once.
            VersionedRecord<EventStateProgressionState> latest = stateStore.get(record.key());
            if (latest == null) {
                return;
            }
            long streamTime = context().currentStreamTimeMs();
            if (streamTime > latest.timestamp() + parameters.getBufferGracePeriodMs()) {
                return;
            }
        } else {
            latestPhaseStore.put(record.key(), current);
            stateStore.put(record.key(), current, record.timestamp());
        }

        // Query the buffer, excluding the grace period relative to stream time "now".
        Instant excludeGracePeriod =
                Instant.ofEpochMilli(context().currentStreamTimeMs())
                        .minusMillis(parameters.getBufferGracePeriodMs());

        // Start query at the latest transition point to avoid duplicates
        Long latestTransitionTime = latestTransitionStore.get(record.key());
        Instant startTime;
        if (latestTransitionTime != null) {
           startTime = Instant.ofEpochMilli(latestTransitionTime);
        } else {
            // No transitions yet, base start time on time window
            startTime = Instant.ofEpochMilli(context().currentStreamTimeMs())
                    .minusMillis(parameters.getBufferTimeMs());
        }

        // Verify that the exclude grace period is after the start time of the query
        if (excludeGracePeriod.compareTo(startTime) > 0) {
            var query =
                    MultiVersionedKeyQuery.<RsuIntersectionSignalGroupKey, EventStateProgressionState>withKey(record.key())
                            .fromTime(startTime)
                            .toTime(excludeGracePeriod)
                            .withAscendingTimestamps();

            QueryResult<VersionedRecordIterator<EventStateProgressionState>> result =
                    stateStore.query(query,
                            PositionBound.unbounded(),
                            new QueryConfig(false));

            if (result.isSuccess()) {
                try (VersionedRecordIterator<EventStateProgressionState> iterator = result.getResult()) {
                    EventStateProgressionState previousState = null;

                    while (iterator.hasNext()) {
                        final VersionedRecord<EventStateProgressionState> state = iterator.next();
                        final EventStateProgressionState thisState = state.value();
                        if (previousState != null && previousState.getPhaseState() != thisState.getPhaseState()) {

                            if (parameters.isDebug()) {
                                log.info("transition detected at timestamp {} -> {}, signal group {}, {} -> {}",
                                        previousState.getUtcTimeStamp(), state.timestamp(),
                                        record.key().getSignalGroup(), previousState.getPhaseState(), thisState.getPhaseState());
                            }

                            latestTransitionStore.put(record.key(), record.timestamp());

                            context().forward(record
                                    .withTimestamp(state.timestamp())
                                    .withValue(new SpatMovementStateTransition(
                                            previousState.toSpatMovementState(),
                                            thisState.toSpatMovementState())));

                        }
                        previousState = thisState;
                    }
                }
            } else {
                log.error("Failed to query state store: {}", result.getFailureMessage());
            }
        } else if (parameters.isDebug()) {
            log.warn("Skipping Query for Event State Progression Processor because start time {} did not happen before end time: {}",
                    startTime, excludeGracePeriod);
        }
    }

    @Override
    public void close() {
        super.close();
    }
}
