package us.dot.its.jpo.conflictmonitor.monitor.models.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.Getter;
import lombok.Setter;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedBasicVehicleRole;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedPrioritizationResponseStatus;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedPriorityRequestType;

import java.util.Set;

import static us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedPrioritizationResponseStatus.*;

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
    private int requestSequenceNumber;

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

    private long requestIngestTime;
    private long responseIngestTime;

    /**
     * Timestamp of the latest SRM for this event
     */
    private long requestTimestamp;
    /**
     * Timestamp of the latest matching SSM for this event
     */
    private long timeOfLastResponse;

    private ProcessedPrioritizationResponseStatus status;

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    public ProcessedPrioritizationResponseStatus getFinalStatus() {
        return hasFinalStatus() ? status : null;
    }

    public PriorityPreemptionRequestEvent() {
        super("PriorityPreemptionRequest");
    }

    public boolean hasFinalStatus() {
        return status != null && FINAL_STATUSES.contains(status);
    }

    public final static Set<ProcessedPrioritizationResponseStatus> FINAL_STATUSES =
            Set.of(GRANTED, REJECTED, MAXPRESENCE, RESERVICELOCKED);



}
