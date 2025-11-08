package us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class JoinedRequestStatus {
    private SrmRequest srmRequest;
    private SsmStatus ssmStatus;
    public PriorityPreemptionRequestEvent toEvent() {
        var event = new PriorityPreemptionRequestEvent();
        var request = getSrmRequest();
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
        } else {
            event.setIntersectionID(0);
            event.setRoadRegulatorID(0);
            event.setVehicleId(null);
            event.setRequestTimestamp(0L);
            event.setPriorityRequestType(null);
            event.setVehicleType(null);
            event.setPriorityRequestType(null);
            event.setInboundLaneId(null);
            event.setInboundApproachId(null);
            event.setInboundLaneConnectionId(null);
            event.setOutboundLaneId(null);
            event.setOutboundApproachId(null);
            event.setOutboundLaneConnectionId(null);
            event.setRequestId(0);
            event.setRequestIngestTime(0L);
        }
        var status = getSsmStatus();
        if (status != null) {
            event.setTimeOfLastResponse(status.getTimestamp());
            event.setResponseIngestTime(status.getIngestTime());
            event.setStatus(status.getStatus());
        } else {
            event.setTimeOfLastResponse(0L);
            event.setResponseIngestTime(0L);
            event.setStatus(null);
        }
        return event;
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
