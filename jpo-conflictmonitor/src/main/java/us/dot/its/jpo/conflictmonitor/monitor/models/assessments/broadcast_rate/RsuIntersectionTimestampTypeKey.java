package us.dot.its.jpo.conflictmonitor.monitor.models.assessments.broadcast_rate;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.TimestampType;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;

/**
 * Key that includes timestamp type for broadcast rate assessments
 */
@EqualsAndHashCode(callSuper = true)
@Data
@ToString(callSuper = true)
public class RsuIntersectionTimestampTypeKey extends RsuIntersectionKey {

    TimestampType timestampType;

    public RsuIntersectionTimestampTypeKey() {}

    public RsuIntersectionTimestampTypeKey(RsuIntersectionKey intersectionKey, TimestampType timestampType) {
        super(intersectionKey.getRsuId(), intersectionKey.getIntersectionId());
        this.timestampType = timestampType;
    }
}
