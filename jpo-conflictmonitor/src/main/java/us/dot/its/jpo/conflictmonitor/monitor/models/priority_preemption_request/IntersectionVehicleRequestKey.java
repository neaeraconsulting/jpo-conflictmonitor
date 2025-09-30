package us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request;

import lombok.Data;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedSignalRequest;
import us.dot.its.jpo.geojsonconverter.pojos.ssm.ProcessedSignalStatus;

@Data
public class IntersectionVehicleRequestKey implements IntersectionKey {

    public IntersectionVehicleRequestKey() {}

    public IntersectionVehicleRequestKey(final String vehicleId, final ProcessedSignalRequest request) {
        this.vehicleId = vehicleId;
        intersectionId = request.getIntersectionId();
        region = request.getRegion();
        requestId = request.getRequestID();
    }

    public IntersectionVehicleRequestKey(final Integer intersectionId, final Integer region, final ProcessedSignalStatus status) {
        this.intersectionId = intersectionId;
        this.region = region;
        vehicleId = status.getVehicleID();
        requestId = status.getRequestID();
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
