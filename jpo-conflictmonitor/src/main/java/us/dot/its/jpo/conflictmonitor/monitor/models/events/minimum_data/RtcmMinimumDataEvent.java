package us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data;

import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@Generated
public class RtcmMinimumDataEvent extends BaseMinimumDataEvent {
    public RtcmMinimumDataEvent() {
        super("RtcmMinimumData");
    }

    /**
     * The RTCM Station ID
     */
    private int stationId;
}
