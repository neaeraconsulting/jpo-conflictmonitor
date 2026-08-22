package us.dot.its.jpo.conflictmonitor.monitor.models.map;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;

/**
 * Timestamp extractor for {@link ProcessedMap} messages.
 * Uses the ODE "Received At" time because the "minute of year" timestamp in MAPs is optional
 * and its behavior is not well defined.  It is often the time when the MAP was first
 * created, and does not contain any information about what the current year is.
 */
@Slf4j
public class MapTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof ProcessedMap<?> processedMap) {
            return getMapReceivedAtTimestamp(processedMap, partitionTime);
        }
        return partitionTime;
    }

    private static <TGeom> long  getMapReceivedAtTimestamp(ProcessedMap<TGeom> processedMap, long partitionTime) {
        try {
            if (processedMap.getProperties() == null || processedMap.getProperties().getOdeReceivedAt() == null) {
                log.error("MAP is missing OdeReceivedAt property {}", processedMap);
                return partitionTime;
            }
            return processedMap.getProperties().getOdeReceivedAt().toInstant().toEpochMilli();
        } catch (Exception e) {
            log.error("Error extracting timestamp from MAP", e);
            return partitionTime;
        }
    }
}
