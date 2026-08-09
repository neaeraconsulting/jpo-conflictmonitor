package us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate;

import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.Getter;
import lombok.Setter;
import us.dot.its.jpo.geojsonconverter.standards.SpatStandard;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@Generated
public class SpatBroadcastRateEvent
    extends BroadcastRateEvent {

    public SpatBroadcastRateEvent() {
        super("SpatBroadcastRate");
    }

    private SpatStandard standard;

}
