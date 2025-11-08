package us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;

import java.util.Objects;

@Data
@NoArgsConstructor
@Slf4j
public class JoinedRequestStatus {
    public JoinedRequestStatus(SrmRequest srmRequest, SsmStatus ssmStatus) {
        this.srmRequest = srmRequest;
        this.ssmStatus = ssmStatus;
        validate();
    }
    private SrmRequest srmRequest;
    private SsmStatus ssmStatus;

    public PriorityPreemptionRequestEvent toEvent() {
        var event = new PriorityPreemptionRequestEvent();
        var request = getSrmRequest();
        var status = getSsmStatus();
        if (request != null) {
            event.setIntersectionID(request.getIntersectionId());
            event.setRoadRegulatorID(request.getRegion());
            event.setVehicleId(request.getVehicleId());
            event.setRequestTimestamp(request.getTimestamp());
            event.setPriorityRequestType(request.getRequestType());
            event.setVehicleType(request.getVehicleType());
            event.setPriorityRequestType(request.getRequestType());
            event.setInboundLaneId(request.getInboundLaneId());
            event.setInboundApproachId(request.getInboundApproachId());
            event.setInboundLaneConnectionId(request.getInboundLaneConnectionId());
            event.setOutboundLaneId(request.getOutboundLaneId());
            event.setOutboundApproachId(request.getOutboundApproachId());
            event.setOutboundLaneConnectionId(request.getOutboundLaneConnectionId());
            event.setRequestId(request.getRequestId());
            event.setRequestIngestTime(request.getIngestTime());
            event.setRequestSequenceNumber(request.getRequestSequenceNumber());
        }

        if (status != null) {
            event.setTimeOfLastResponse(status.getTimestamp());
            event.setResponseIngestTime(status.getIngestTime());
            event.setStatus(status.getStatus());
            event.setRequestSequenceNumber(status.getRequestSequenceNumber());
        }
        return event;
    }

    public void setSrmRequest(SrmRequest srmRequest) {
        this.srmRequest = srmRequest;
        validate();
    }

    public void setSsmStatus(SsmStatus ssmStatus) {
        this.ssmStatus = ssmStatus;
        validate();
    }


    private void validate() {
        var request = getSrmRequest();
        var status = getSsmStatus();
        if (request != null && status != null) {
            // key items must match
            if (request.getRequestId() != status.getRequestId()) {
                throw new IllegalArgumentException("requestId differs between request and status");
            }
            if (!StringUtils.equals(request.getVehicleId(), status.getVehicleId())) {
                throw new IllegalArgumentException("vehicleId differs between request and status");
            }
            if (request.getRequestSequenceNumber() != status.getRequestSequenceNumber()) {
                throw new IllegalArgumentException("requestSequenceNumber differs between request and status");
            }
        }
    }

    @Override
    public String toString() {
        try {
            return DateJsonMapper.getInstance().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Exception serializing to JSON", e);
        }
        return "";
    }
}
