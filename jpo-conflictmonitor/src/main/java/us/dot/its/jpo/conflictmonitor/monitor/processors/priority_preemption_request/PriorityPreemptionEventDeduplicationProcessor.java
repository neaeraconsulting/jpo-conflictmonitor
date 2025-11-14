package us.dot.its.jpo.conflictmonitor.monitor.processors.priority_preemption_request;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.IntersectionVehicleRequestSequenceKey;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

@Slf4j
public class PriorityPreemptionEventDeduplicationProcessor
    extends ContextualProcessor<
            IntersectionVehicleRequestSequenceKey,
            Pair<PriorityPreemptionRequestEvent, Long>,
            IntersectionVehicleRequestSequenceKey,
            PriorityPreemptionRequestEvent
            > {

    public PriorityPreemptionEventDeduplicationProcessor(PriorityPreemptionRequestParameters parameters) {
        this.parameters = parameters;
        this.retentionTime = Duration.of(parameters.getStoreRetentionTime(), parameters.getRetentionTimeUnits());
        this.deduplicateEventsStoreName = parameters.getDeduplicateEventsStoreName();
    }

    private final PriorityPreemptionRequestParameters parameters;
    private final Duration retentionTime;
    private final String deduplicateEventsStoreName;

    // KTable store is value-and-timestamp store
    KeyValueStore<IntersectionVehicleRequestSequenceKey, ValueAndTimestamp<Long>> dedupStore;

    @Override
    public void init(ProcessorContext<IntersectionVehicleRequestSequenceKey, PriorityPreemptionRequestEvent> context) {
        super.init(context);
        context().schedule(retentionTime, PunctuationType.WALL_CLOCK_TIME, this::punctuate);
        dedupStore = context().getStateStore(deduplicateEventsStoreName);
        if (dedupStore == null) {
            log.error("Dedup store not found in processor");
        }
    }

    @Override
    public void process(org.apache.kafka.streams.processor.api.Record<IntersectionVehicleRequestSequenceKey, Pair<PriorityPreemptionRequestEvent, Long>> record) {
        Pair<PriorityPreemptionRequestEvent, Long> timestampedEvent = record.value();
        final Long eventTimestamp = timestampedEvent.getRight();
        final Instant eventTime = eventTimestamp != null ? Instant.ofEpochMilli(eventTimestamp) : null;
        final Instant wallClockTime = Instant.ofEpochMilli(context().currentSystemTimeMs());
        // Forward the record if it doesn't have an entry in the deduplicate table, or if the entry
        // has a timestamp longer ago than the retention time
        if (eventTime == null || eventTime.plus(retentionTime).isBefore(wallClockTime)) {
            var newRecord = new org.apache.kafka.streams.processor.api.Record<>(record.key(), record.value().getLeft(), record.timestamp());
            context().forward(newRecord);
            if (parameters.isDebug()) {
                log.info("deduplicator forwarded {}", newRecord.key());
            }
        } else {
            if (parameters.isDebug()) {
                log.info("deduplicator filtered out key {}", record.key());
            }
        }
    }

    // Punctuator checks if retention time is passed for each key and sends a tombstone to the topic
    // to clear the deduplicator ktable store
    private void punctuate(long punctuationTime) {
        var keysToDelete = new ArrayList<IntersectionVehicleRequestSequenceKey>();
        try (KeyValueIterator<IntersectionVehicleRequestSequenceKey, ValueAndTimestamp<Long>> iterator = dedupStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<IntersectionVehicleRequestSequenceKey, ValueAndTimestamp<Long>> item = iterator.next();
                final var key = item.key;
                final var value = item.value;
                final Long storedTimestamp = value != null ? value.value() : null;
                if (storedTimestamp == null) continue;
                final Instant eventTime = Instant.ofEpochMilli(storedTimestamp);
                final Instant wallClockTime = Instant.ofEpochMilli(context().currentSystemTimeMs());
                if (eventTime.plus(retentionTime).isBefore(wallClockTime)) {
                    if (parameters.isDebug()) {
                        log.info("punctuator will delete key {}.  eventTime {} plus retentionTime {} is before wallClockTime {}",
                                key, eventTime, retentionTime, wallClockTime);
                    }
                    keysToDelete.add(key);
                }
            }
        }
        for (IntersectionVehicleRequestSequenceKey key : keysToDelete) {
            var tombstoneRecord = new Record<>(key, (PriorityPreemptionRequestEvent)null, punctuationTime);
            context().forward(tombstoneRecord);
        }
    }
}
