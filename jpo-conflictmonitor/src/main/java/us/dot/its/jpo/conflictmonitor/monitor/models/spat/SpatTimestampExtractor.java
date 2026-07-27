package us.dot.its.jpo.conflictmonitor.monitor.models.spat;

import java.time.ZonedDateTime;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

public class SpatTimestampExtractor implements TimestampExtractor {

    private static final Logger logger = LoggerFactory.getLogger(SpatTimestampExtractor.class);

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        ProcessedSpat spat = (ProcessedSpat) record.value();
        if (spat != null) {
            return getSpatTimestamp(spat);
        }

        return partitionTime;
    }

    public static long getSpatTimestamp(ProcessedSpat spat) {
        try {
            if (spat.getUtcTimeStamp() != null) {
                ZonedDateTime time = spat.getUtcTimeStamp();
                return time.toInstant().toEpochMilli();
            }
            logger.debug("SPaT timestamp parsing failed: utcTimeStamp was null");
            return -1;
        } catch (Exception e) {
            logger.debug("SPaT timestamp parsing failed: {}", e.getMessage());
            return -1;
        }
    }
}
