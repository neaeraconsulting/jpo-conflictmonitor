package us.dot.its.jpo.conflictmonitor.monitor.models.notifications.broadcast_rate;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import us.dot.its.jpo.conflictmonitor.monitor.models.assessments.broadcast_rate.SpatBroadcastRateAssessment;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SpatBroadcastRateNotification extends BroadcastRateNotification<SpatBroadcastRateAssessment> {

    public SpatBroadcastRateNotification() {
        super("SpatBroadcastRateNotification");
    }
    
}
