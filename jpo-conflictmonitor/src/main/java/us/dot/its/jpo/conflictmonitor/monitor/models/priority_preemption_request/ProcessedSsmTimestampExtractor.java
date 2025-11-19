package us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import us.dot.its.jpo.geojsonconverter.pojos.ssm.ProcessedSsm;

import java.time.ZonedDateTime;

public class ProcessedSsmTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() != null && record.value() instanceof ProcessedSsm processedSsm) {
            Long timestamp = getSsmTimestamp(processedSsm);
            if (timestamp != null) {
                return timestamp;
            }
        }
        return partitionTime;
    }

    private static Long getSsmTimestamp(ProcessedSsm processedSsm) {
        ZonedDateTime zdtTimestamp = processedSsm.getTimeStamp();
        if (zdtTimestamp != null) {
            return zdtTimestamp.toInstant().toEpochMilli();
        }
        ZonedDateTime receivedAt = processedSsm.getOdeReceivedAt();
        if (receivedAt != null) {
            return receivedAt.toInstant().toEpochMilli();
        }
        return null;
    }
}
