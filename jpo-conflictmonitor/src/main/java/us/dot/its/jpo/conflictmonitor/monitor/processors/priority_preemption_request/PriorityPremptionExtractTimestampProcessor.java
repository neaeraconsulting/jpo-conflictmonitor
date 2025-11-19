package us.dot.its.jpo.conflictmonitor.monitor.processors.priority_preemption_request;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.IntersectionVehicleRequestSequenceKey;

@Slf4j
public class PriorityPremptionExtractTimestampProcessor
    extends ContextualProcessor<
        IntersectionVehicleRequestSequenceKey,
        PriorityPreemptionRequestEvent,
        IntersectionVehicleRequestSequenceKey,
        Long> {

    final PriorityPreemptionRequestParameters parameters;

    public PriorityPremptionExtractTimestampProcessor(PriorityPreemptionRequestParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public void process(org.apache.kafka.streams.processor.api.Record<IntersectionVehicleRequestSequenceKey, PriorityPreemptionRequestEvent> record) {
        final var key = record.key();
        final var streamTime = record.timestamp();
        final var wallClockTime = context().currentSystemTimeMs();
        final var value = record.value();
        if (value == null) {
            // Pass tombstones through to clear the ktable store
            var tombstoneRecord = new org.apache.kafka.streams.processor.api.Record<>(key, (Long)null, streamTime);
            if (parameters.isDebug()) {
                log.debug("Processor passing through tombstone record for key: {}", key);
            }
            context().forward(tombstoneRecord);
        } else {
            // Value of new record is wall clock timestamp
            var newRecord = new Record<>(key, wallClockTime, streamTime);
            context().forward(newRecord);
        }
    }

}
