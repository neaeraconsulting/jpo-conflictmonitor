package us.dot.its.jpo.conflictmonitor.monitor.processors;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import us.dot.its.jpo.conflictmonitor.monitor.models.spat.SpatTimestampExtractor;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

/**
 * Forwards each SPaT with stream time set to {@code utcTimeStamp} (event time)
 * so plugins that expect event-time semantics can share one consume with
 * odeReceivedAt-based broadcast-rate validation.
 */
public class ReassignSpatEventTimestampProcessor
        extends ContextualProcessor<RsuIntersectionKey, ProcessedSpat, RsuIntersectionKey, ProcessedSpat> {

    @Override
    public void process(Record<RsuIntersectionKey, ProcessedSpat> record) {
        ProcessedSpat spat = record.value();
        if (spat == null) {
            return;
        }
        long eventTime = SpatTimestampExtractor.getSpatTimestamp(spat);
        if (eventTime < 0) {
            eventTime = record.timestamp();
        }
        context().forward(record.withTimestamp(eventTime));
    }
}
