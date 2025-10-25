package us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;

@Data
@NoArgsConstructor
@AllArgsConstructor
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
        }
        var status = getSsmStatus();
        if (status != null) {
            event.setRequestId(status.getRequestId());
            event.setTimeOfLastResponse(status.getTimestamp());
            event.setStatus(status.getStatus());
        }
        return event;
    }
}
