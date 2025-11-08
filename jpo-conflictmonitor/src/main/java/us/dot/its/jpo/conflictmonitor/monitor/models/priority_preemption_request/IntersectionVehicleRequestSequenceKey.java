package us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedSignalRequest;
import us.dot.its.jpo.geojsonconverter.pojos.ssm.ProcessedSignalStatus;

/**
 * VehicleID, RequestID, and Request Sequence Number are used to match status responses to requests.
 * Per J2735 (2024) section 6.130:
 * <pre>
 * -- These three items serve to uniquely identify the requester
 * -- and the specific request to all parties:
 * id VehicleID
 * request RequestID
 * sequenceNumber MsgCount
 * </pre>
 * <p>Note this key is used to match requests and responses to ensure that responses are matched to requests with the
 * correct status, but not used for the fulfillment rate metric, because the request sequence number may be incremented
 * when fields that are not significant for the metric, such as lon/lat coordinates, change.
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class IntersectionVehicleRequestSequenceKey extends IntersectionVehicleRequestKey {

    // Parameterless constructor for Jackson
    public IntersectionVehicleRequestSequenceKey() {}

    public IntersectionVehicleRequestSequenceKey(final String vehicleId, final ProcessedSignalRequest request, final int requestSequenceNumber) {
        super(vehicleId, request);
        this.requestSequenceNumber = requestSequenceNumber;
    }

    public IntersectionVehicleRequestSequenceKey(final Integer intersectionId, final Integer region, final ProcessedSignalStatus status) {
        super(intersectionId, region, status);
        this.requestSequenceNumber = status.getRequesterSequenceNumber();
    }

    private int requestSequenceNumber;
}
