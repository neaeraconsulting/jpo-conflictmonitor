package us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request;

import lombok.Data;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedPriorityRequestType;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedSignalRequest;

/**
 * A flattened object with one SRM status response and key fields from the SRM that contains it.
 */
@Data
public class SrmRequest {

    public SrmRequest() {}

    public SrmRequest(final String vehicleId, final long timestamp, final ProcessedSignalRequest request) {
        this.vehicleId = vehicleId;
        this.timestamp = timestamp;
        intersectionId = request.getIntersectionId();
        region = request.getRegion();
        requestId = request.getRequestID();
        inboundLaneId = request.getInboundLaneID();
        inboundApproachId = request.getInboundApproachID();
        inboundLaneConnectionId = request.getInboundLaneConnectionID();
        outboundLaneId = request.getOutboundLaneID();
        outboundApproachId = request.getOutboundApproachID();
        outboundLaneConnectionId = request.getOutboundLaneConnectionID();
        requestType = request.getPriorityRequestType();
    }

    private String vehicleId;
    private long timestamp;
    private int intersectionId;
    private int region;
    private int requestId;
    private Integer inboundLaneId;
    private Integer inboundApproachId;
    private Integer inboundLaneConnectionId;
    private Integer outboundLaneId;
    private Integer outboundApproachId;
    private Integer outboundLaneConnectionId;
    private ProcessedPriorityRequestType requestType;
}
