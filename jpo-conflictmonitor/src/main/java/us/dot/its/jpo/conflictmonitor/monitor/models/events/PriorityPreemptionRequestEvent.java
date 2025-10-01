package us.dot.its.jpo.conflictmonitor.monitor.models.events;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.Getter;
import lombok.Setter;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedBasicVehicleRole;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedPrioritizationResponseStatus;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedPriorityRequestType;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
@EqualsAndHashCode(callSuper=true)
@Generated
public class PriorityPreemptionRequestEvent
    extends Event {

    private String vehicleId;
    private ProcessedBasicVehicleRole vehicleType;
    private int requestId;
    private long requestTimestamp;
    private ProcessedPriorityRequestType priorityRequestType;

    // Inbound Intersection Access Point
    // One of inbound LaneID, ApproachID, or LaneConnectionID is required
    private Integer inboundLaneId;
    private Integer inboundApproachId;
    private Integer inboundLaneConnectionId;

    // Outbound access point ID is optional
    private Integer outboundLaneId;
    private Integer outboundApproachId;
    private Integer outboundLaneConnectionId;

    private long timeOfLastResponse;

    private ProcessedPrioritizationResponseStatus status;
    private ProcessedPrioritizationResponseStatus finalStatus;

    public PriorityPreemptionRequestEvent() {
        super("PriorityPreemptionRequest");
    }

    @JsonIgnore
    public boolean isFinalStatus() {
        return finalStatus != null;
    }


}
