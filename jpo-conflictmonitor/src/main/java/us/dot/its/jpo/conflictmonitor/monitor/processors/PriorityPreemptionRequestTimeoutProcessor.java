package us.dot.its.jpo.conflictmonitor.monitor.processors;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.IntersectionVehicleRequestSequenceKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.JoinedRequestStatus;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.SrmRequest;

import java.time.Duration;
import java.util.ArrayList;

/**
 * Processor to periodically check the SRM/SSM joined table for SRMs with no SSM response
 * or no final status after the max time and emit events.  Maintains a key-value state store
 * for the joined KTable, with a clock-time punctuator running the timeout check and cleanup.
 */
@Slf4j
public class PriorityPreemptionRequestTimeoutProcessor
    extends ContextualProcessor<IntersectionVehicleRequestSequenceKey, JoinedRequestStatus,
        IntersectionVehicleRequestSequenceKey, PriorityPreemptionRequestEvent> {

    private final String joinedStoreName;
    private final Duration maxTimeBetweenSrms;
    private final long maxTimeBetweenSrmsMillis;
    private KeyValueStore<IntersectionVehicleRequestSequenceKey, JoinedRequestStatus> joinedStore;
    private final boolean isDebug;
    ProcessorContext<IntersectionVehicleRequestSequenceKey, PriorityPreemptionRequestEvent> context;

    public PriorityPreemptionRequestTimeoutProcessor(Duration maxTimeBetweenSrms, boolean isDebug, String joinedStoreName) {
        this.maxTimeBetweenSrms = maxTimeBetweenSrms;
        this.maxTimeBetweenSrmsMillis = maxTimeBetweenSrms.toMillis();
        this.joinedStoreName = joinedStoreName;
        this.isDebug = isDebug;
    }

    @Override
    public void init(ProcessorContext<IntersectionVehicleRequestSequenceKey, PriorityPreemptionRequestEvent> context) {
        super.init(context);
        this.context = context;
        joinedStore = context.getStateStore(joinedStoreName);
        context.schedule(maxTimeBetweenSrms, PunctuationType.WALL_CLOCK_TIME, this::punctuate);
    }

    @Override
    public void process(Record<IntersectionVehicleRequestSequenceKey, JoinedRequestStatus> record) {

        if (isDebug) {
            log.debug("process JoinedRequestStatus key: {}, value: {}", record.key(), record.value());
        }

        var value = record.value();

        if (value == null) {
            // Delete if tombstone
            joinedStore.delete(record.key());
            if (isDebug) {
                log.debug("deleted tombstone");
            }
            return;
        };

        // SRM is present with final SSM, this processor doesn't care
        if (value.getSsmStatus() != null && value.getSsmStatus().isFinalStatus()) {
            // Delete without event if final status (the main topology will emit the event)
            joinedStore.delete(record.key());
            if (isDebug) {
                log.debug("deleted final value");
            }
            return;
        };

        if (value.getSrmRequest() != null && value.getSrmRequest().getTimestamp() > 0) {
            // This is an SRM without an SSM, or with a non-final status, save it
            joinedStore.put(record.key(), record.value());
            if (isDebug) {
                log.debug("stored joined value");
            }
        } else {
            // This is an SSM without an SRM, or without a valid timestamp,
            // don't save it because it doesn't matter for the fulfillment metric
            if (isDebug) {
                log.debug("SSM without SRM, not storing it");
            }
        }

    }

    private void punctuate(long timestamp) {
        if (isDebug) {
            log.trace("punctuate {}", timestamp);
        }
        var keysToDelete = new ArrayList<IntersectionVehicleRequestSequenceKey>();
        try (var storeInterator = joinedStore.all()) {
            while (storeInterator.hasNext()) {
                KeyValue<IntersectionVehicleRequestSequenceKey, JoinedRequestStatus> item = storeInterator.next();
                IntersectionVehicleRequestSequenceKey key = item.key;
                JoinedRequestStatus joined = item.value;
                SrmRequest request = joined.getSrmRequest();
                long requestTimestamp = request.getTimestamp();
                if (isDebug) {
                    log.debug("timestamp {}, requestTimestamp {}, diff {}, maxTimeBetweenSrmsMillis {}", timestamp,
                            requestTimestamp, timestamp - requestTimestamp, maxTimeBetweenSrmsMillis);
                }
                if (timestamp - requestTimestamp > maxTimeBetweenSrmsMillis) {
                    // Forward event if timed out and not already emitted for this key
                    PriorityPreemptionRequestEvent event = joined.toEvent();
                    context.forward(new Record<>(key, event, timestamp));
                    if (isDebug) {
                        log.info("Emitted event: key: {}, value: {}", key, event);
                    }
                    keysToDelete.add(key);
                }
            }
        }
        // Clean up
        for (IntersectionVehicleRequestSequenceKey key : keysToDelete) {
            joinedStore.delete(key);
        }
    }

}
