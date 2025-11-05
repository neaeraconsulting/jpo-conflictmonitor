package us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request;

import lombok.Data;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedBasicVehicleRole;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedPriorityRequestType;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedSignalRequest;

/**
 * A flattened object with one SRM status response and key fields from the SRM that contains it.
 */
@Data
public class SrmRequest {

    public SrmRequest() {}

    public SrmRequest(final String vehicleId, final ProcessedBasicVehicleRole vehicleType,
                      final long timestamp, final ProcessedSignalRequest request) {
        this.vehicleId = vehicleId;
        this.timestamp = timestamp;
        this.vehicleType = vehicleType;
        intersectionId = request.getIntersectionId() != null ? request.getIntersectionId() : -1;
        region = request.getRegion() != null ? request.getRegion() : -1;
        requestId = request.getRequestID() != null ? request.getRequestID() : -1;
        inboundLaneId = request.getInboundLaneID();
        inboundApproachId = request.getInboundApproachID();
        inboundLaneConnectionId = request.getInboundLaneConnectionID();
        outboundLaneId = request.getOutboundLaneID();
        outboundApproachId = request.getOutboundApproachID();
        outboundLaneConnectionId = request.getOutboundLaneConnectionID();
        requestType = request.getPriorityRequestType();
    }

    private String vehicleId;
    private ProcessedBasicVehicleRole vehicleType;
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
