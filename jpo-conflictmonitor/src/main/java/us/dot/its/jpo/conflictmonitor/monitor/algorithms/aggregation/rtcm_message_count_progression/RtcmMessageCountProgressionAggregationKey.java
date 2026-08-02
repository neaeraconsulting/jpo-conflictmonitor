package us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.rtcm_message_count_progression;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Generated;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;

import java.util.List;

@Data
@EqualsAndHashCode(callSuper = true)
@Generated
public class RtcmMessageCountProgressionAggregationKey extends RsuStationIdKey {
    private List<String> change;
}
