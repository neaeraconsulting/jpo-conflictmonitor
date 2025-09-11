package us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate;

import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@Generated
public class RtcmBroadcastRateEvent
        extends BroadcastRateEvent {
    public RtcmBroadcastRateEvent() {
        super("RtcmBroadcastRate");
    }

    private int stationId;
}
