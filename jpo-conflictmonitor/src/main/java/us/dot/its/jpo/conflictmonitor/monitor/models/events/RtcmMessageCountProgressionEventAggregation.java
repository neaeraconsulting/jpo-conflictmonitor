package us.dot.its.jpo.conflictmonitor.monitor.models.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RtcmMessageCountProgressionEventAggregation
    extends EventAggregation<RtcmMessageCountProgressionEvent> {

    public RtcmMessageCountProgressionEventAggregation() {
        super("RtcmMessageCountProgressionAggregation");
    }

    private final String messageType = "RTCM";
    private final String dataFrame = "MSG_RTCMcorrections";
    private List<String> change;

    @Override
    public void update(RtcmMessageCountProgressionEvent event) {
        setNumberOfEvents(getNumberOfEvents() + 1);
    }
}
