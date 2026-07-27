package us.dot.its.jpo.conflictmonitor.monitor.models.bsm;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import us.dot.its.jpo.geojsonconverter.pojos.geojson.bsm.ProcessedBsm;

public class ProcessedBsmTimestampExtractor implements TimestampExtractor {
    
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof ProcessedBsm<?> message
                 && message.getProperties() != null
                 && message.getProperties().getTimeStamp() != null) {
             return message.getProperties().getTimeStamp().toInstant().toEpochMilli();
         }
         return partitionTime;
    }
}