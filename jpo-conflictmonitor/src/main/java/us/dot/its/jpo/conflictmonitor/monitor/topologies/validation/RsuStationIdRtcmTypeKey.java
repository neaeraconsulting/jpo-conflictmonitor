package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;


import lombok.*;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;

/**
 * Key to distinguish the type of RTCM: whether it contains MSM
 * message types, which are counted at a different rate than basic message types.
 * See CTI-4501 4.3.3.5.2.2 and
 * <a href="https://www.use-snip.com/kb/knowledge-base/an-rtcm-message-cheat-sheet/">an-rtcm-message-cheat-sheet</a>
 */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@Getter
@Setter
public class RsuStationIdRtcmTypeKey extends RsuStationIdKey {

    public RsuStationIdRtcmTypeKey() {
        super();
    }

    public RsuStationIdRtcmTypeKey(RsuStationIdKey key) {
        setRsuId(key.getRsuId());
        setStationId(key.getStationId());
    }

    private boolean includesMSMTypes;

    public RsuStationIdKey rsuStationIdKey() {
        var key = new RsuStationIdKey();
        key.setRsuId(getRsuId());
        key.setStationId(getStationId());
        return key;
    }
}
