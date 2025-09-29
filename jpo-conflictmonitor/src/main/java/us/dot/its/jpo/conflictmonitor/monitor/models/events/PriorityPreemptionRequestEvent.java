package us.dot.its.jpo.conflictmonitor.monitor.models.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.Getter;
import lombok.Setter;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.IntersectionAccessPointType;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
@EqualsAndHashCode(callSuper=true)
@Generated
public class PriorityPreemptionRequestEvent
    extends Event {

    private String vehicleId;
    private int requestId;
    private long requestTimestamp;
    private String priorityRequestType;

    // Inbound Intersection Access Point
    // One of inbound LaneID, ApproachID, or LaneConnectionID is required
    private Integer inboundLaneId;
    private Integer inboundApproachId;
    private Integer inboundLaneConnectionId;

    // Outbound access point ID is optional
    private Integer oubboundLaneId;
    private Integer outboundApproachId;
    private Integer outboundLaneConnectionId;

    private long timeOfLastResponse;

    private String status;
    private String finalStatus;

    public PriorityPreemptionRequestEvent() {
        super("PriorityPreemptionRequest");
    }


}
