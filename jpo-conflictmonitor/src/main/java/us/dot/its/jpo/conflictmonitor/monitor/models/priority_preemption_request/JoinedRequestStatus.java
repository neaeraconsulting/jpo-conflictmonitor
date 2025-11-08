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
        }
        var status = getSsmStatus();
        if (status != null) {
            event.setTimeOfLastResponse(status.getTimestamp());
            event.setStatus(status.getStatus());
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
