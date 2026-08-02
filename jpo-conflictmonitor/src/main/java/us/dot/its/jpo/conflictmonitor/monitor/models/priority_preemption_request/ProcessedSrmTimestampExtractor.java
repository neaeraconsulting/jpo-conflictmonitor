package us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedSrm;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.SrmProperties;

import java.time.ZonedDateTime;

public class ProcessedSrmTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() != null && record.value() instanceof ProcessedSrm processedSrm) {
            Long timestamp = getSrmTimestamp(processedSrm);
            if (timestamp != null) {
                return timestamp;
            }
        }
        return partitionTime;
    }

    private static Long getSrmTimestamp(ProcessedSrm processedSrm) {
        SrmProperties properties = processedSrm.getProperties();
        if (properties != null) {
            ZonedDateTime zdtTimestamp = properties.getTimeStamp();
            if (zdtTimestamp != null) {
                return zdtTimestamp.toInstant().toEpochMilli();
            }
            ZonedDateTime receivedAt = properties.getOdeReceivedAt();
            if (receivedAt != null) {
                return receivedAt.toInstant().toEpochMilli();
            }
        }
        return null;
    }
}
