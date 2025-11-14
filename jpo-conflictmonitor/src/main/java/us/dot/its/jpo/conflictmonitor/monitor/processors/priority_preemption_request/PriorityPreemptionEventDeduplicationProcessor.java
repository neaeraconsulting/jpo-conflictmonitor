package us.dot.its.jpo.conflictmonitor.monitor.processors.priority_preemption_request;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
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
            PriorityPreemptionRequestEvent,
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

    KeyValueStore<IntersectionVehicleRequestSequenceKey, Long> dedupStore;

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
    public void process(org.apache.kafka.streams.processor.api.Record<IntersectionVehicleRequestSequenceKey, PriorityPreemptionRequestEvent> record) {
        final IntersectionVehicleRequestSequenceKey key = record.key();
        final Long storedTimestamp = dedupStore.get(key);
        final boolean isStored = storedTimestamp != null;
        final Instant storedTime = isStored ? Instant.ofEpochMilli(storedTimestamp) : null;
        final long eventTimestamp = record.timestamp();
        final Instant eventTime = Instant.ofEpochMilli(eventTimestamp);
        final Instant wallClockTime = Instant.ofEpochMilli(context().currentSystemTimeMs());
        final boolean isStoredOld = isStored && storedTime.plus(retentionTime).isBefore(wallClockTime);

        // Remove the key now if it's old
        if (isStoredOld) {
            dedupStore.delete(key);
        }

        // Store it if it isn't already
        if (!isStored) {
            dedupStore.put(key, eventTimestamp);
        }

        // Forward the record along if it doesn't have an entry in the deduplicate store, or if the entry
        // has a timestamp longer ago than the retention time
        if (!isStored || isStoredOld) {
            context().forward(record);
            if (parameters.isDebug()) {
                log.info("deduplicator forwarded {}", record.key());
            }
        } else {
            if (parameters.isDebug()) {
                log.info("deduplicator filtered out key {}", record.key());
            }
        }
    }

    // Punctuator checks if retention time is passed for each key and removes old keys from the store
    private void punctuate(long punctuationTime) {
        var keysToDelete = new ArrayList<IntersectionVehicleRequestSequenceKey>();
        try (KeyValueIterator<IntersectionVehicleRequestSequenceKey, Long> iterator = dedupStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<IntersectionVehicleRequestSequenceKey, Long> item = iterator.next();
                final var key = item.key;
                final Long storedTimestamp = item.value;
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
            dedupStore.delete(key);
        }
    }
}
