package us.dot.its.jpo.conflictmonitor.monitor.utils;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import us.dot.its.jpo.conflictmonitor.monitor.processors.metrics.TickProcessor;
import us.dot.its.jpo.geojsonconverter.serialization.deserializers.JsonDeserializer;
import us.dot.its.jpo.geojsonconverter.serialization.serializers.JsonSerializer;

/**
 * Record to store stream time and clock time for one message
 * @param streamTime The stream time
 * @param clockTime The clock time
 */
public record Timestamps(long streamTime, long clockTime) {

    /**
     * @return Offset between stream time and clock time
     */
    public long offset() {
        return clockTime - streamTime;
    }

    public static Serde<Timestamps> TimestampsSerdes() {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Timestamps.class));
    }
}
