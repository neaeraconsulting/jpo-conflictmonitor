package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

/**
 * Timestamp Extractor for Broadcast Rate monitoring for Processed Map, Spat, and RTCM messages.
 */
public class TimestampExtractorForBroadcastRate implements TimestampExtractor {
    
    private static final Logger logger = LoggerFactory.getLogger(TimestampExtractorForBroadcastRate.class);

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        var value = record.value();
        
        if (value instanceof ProcessedSpat) {
            var timestamp = extractTimestamp((ProcessedSpat)value);
            if (timestamp > -1) {
                return timestamp;
            }
        } 
        
        if (value instanceof ProcessedMap) {
            var timestamp = extractTimestamp((ProcessedMap)value);
            if (timestamp > -1) {
                return timestamp;
            }
        }

        if (value instanceof ProcessedRTCM processedRTCM) {
            var timestamp = extractTimestamp(processedRTCM);
            if (timestamp > -1) {
                return timestamp;
            }
        }

        if (partitionTime >= 0) {
            logger.warn("Failed to extract timestamp, using partition time");
            return partitionTime;
        }

        logger.warn("Failed to extract timestamp and partition time is invalid, using clock time");
        return Instant.now().toEpochMilli();
     }

    /**
     * For ProcessedSpat, prefer to use the utcTimeStamp derived from the SPAT MinuteOfYear and DSecond timestamps,
     * which are required to be present by CTI-4501, falling back to the ODE Received-At time if those aren't available.
     * @param spat The processed spat
     * @return the extracted timestamp
     */
    public static long extractTimestamp(ProcessedSpat spat) {
        try {
            ZonedDateTime zdt = spat.getUtcTimeStamp();
            if (zdt != null) {
                return zdt.toInstant().toEpochMilli();
            }
            // Fall back to ode received-at if the real timestamp isn't available
            ZonedDateTime receivedAt = ZonedDateTime.parse(spat.getOdeReceivedAt(), DateTimeFormatter.ISO_DATE_TIME);
            return receivedAt.toInstant().toEpochMilli();
        } catch (Exception e){
            logger.error("Timestamp Parsing Failed", e);
            return -1;
        }
    }

    /**
     * For ProcessedMap, out of necessity uses the ODE Received time because MAPs don't have a well-defined timestamp
     * field.
     * @param map the processed map
     * @return the extracted timestamp
     */
    public static long extractTimestamp(ProcessedMap map) {
        try{
            ZonedDateTime zdt = map.getProperties().getOdeReceivedAt();
            long timestamp =  zdt.toInstant().toEpochMilli();
            return timestamp;
        } catch (Exception e){
            logger.error("Timestamp Parsing Failed", e);
            return -1;
        }
    }

    /**
     * For ProcessedRTCM prefer to use the utc timestamp extracted from the RTCM message payload
     * @param rtcm The processed RTCM
     * @return the extracted timestamp
     */
    public static long extractTimestamp(ProcessedRTCM rtcm) {
        try{
            Long utcTime = rtcm.getProperties().getUtcTime();
            if (utcTime != null) {
                return utcTime;
            }
            ZonedDateTime zdt = rtcm.getProperties().getOdeReceivedAt();
            return zdt.toInstant().toEpochMilli();
        } catch (Exception e){
            logger.error("Timestamp Parsing Failed", e);
            return -1;
        }
    }
}
