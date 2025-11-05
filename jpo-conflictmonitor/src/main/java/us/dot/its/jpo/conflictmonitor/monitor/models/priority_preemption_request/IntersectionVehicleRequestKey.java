package us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request;

import lombok.Data;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedSignalRequest;
import us.dot.its.jpo.geojsonconverter.pojos.ssm.ProcessedSignalStatus;

@Data
public class IntersectionVehicleRequestKey implements IntersectionKey {

    public IntersectionVehicleRequestKey() {}

    public IntersectionVehicleRequestKey(final String vehicleId, final ProcessedSignalRequest request) {
        this.intersectionId = request.getIntersectionId() != null ? request.getIntersectionId() : -1;
        this.region = request.getRegion() != null ? request.getRegion() : -1;
        this.vehicleId = vehicleId;
        this.requestId = request.getRequestID() != null ? request.getRequestID() : -1;
    }

    public IntersectionVehicleRequestKey(final Integer intersectionId, final Integer region, final ProcessedSignalStatus status) {
        this.intersectionId = intersectionId != null ? intersectionId : -1;
        this.region = region != null ? region : -1;
        this.vehicleId = status.getVehicleID();
        this.requestId = status.getRequestID() != null ? status.getRequestID() : -1;
    }

    private int intersectionId;
    private int region;
    private String vehicleId;
    private int requestId;

    @Override
    public int getIntersectionId() {
        return intersectionId;
    }

    @Override
    public int getRegion() {
        return region;
    }

}
