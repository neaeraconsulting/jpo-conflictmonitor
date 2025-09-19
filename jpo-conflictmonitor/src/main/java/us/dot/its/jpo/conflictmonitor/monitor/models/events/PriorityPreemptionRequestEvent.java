package us.dot.its.jpo.conflictmonitor.monitor.models.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.Getter;
import lombok.Setter;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
@EqualsAndHashCode(callSuper=true)
@Generated
public class PriorityPreemptionRequestEvent
    extends Event {

    private String vehicleID;
    private int requestID;
    private long requestTimestamp;
    private String priorityRequestType;

    // Intersection Access Point CHOICE can be one of:
    private Integer laneID;
    private Integer approachID;
    private Integer laneConnectionID;

    private long timeOfLastResponse;

    private String status;
    private String finalStatus;

    public PriorityPreemptionRequestEvent() {
        super("PriorityPreemptionRequest");
    }
}
