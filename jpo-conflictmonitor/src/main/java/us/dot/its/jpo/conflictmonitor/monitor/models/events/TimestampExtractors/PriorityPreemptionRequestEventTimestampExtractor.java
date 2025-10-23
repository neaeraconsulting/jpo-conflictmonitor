package us.dot.its.jpo.conflictmonitor.monitor.models.events.TimestampExtractors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;

/**
 * A {@link TimestampExtractor} for aggregating {@link PriorityPreemptionRequestEvent}s.
 */
public class PriorityPreemptionRequestEventTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() != null && record.value() instanceof PriorityPreemptionRequestEvent event) {
            if (event.getTimeOfLastResponse() > 0) {
                // Use time of last response if there was a response
                return event.getTimeOfLastResponse();
            }
            // Otherwise try time of request
            if (event.getRequestTimestamp() > 0) {
                return event.getRequestTimestamp();
            }
            // Or event generation time
            if (event.getEventGeneratedAt() > 0) {
                return event.getEventGeneratedAt();
            }
        }
        // Last resort, partition time
        return partitionTime;
    }
}
