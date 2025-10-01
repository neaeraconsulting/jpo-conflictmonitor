package us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request;

import com.google.common.collect.Sets;
import lombok.Data;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedBasicVehicleRole;
import static us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedPrioritizationResponseStatus.*;

import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedPrioritizationResponseStatus;
import us.dot.its.jpo.geojsonconverter.pojos.ssm.ProcessedSignalStatus;

import java.util.Set;

/**
 * A flattened object with one SSM status and key fields from the SSM that contains it.
 */
@Data
public class SsmStatus {

    public SsmStatus() {}

    public SsmStatus(Integer intersectionId, Integer region, long timestamp, ProcessedSignalStatus signalStatus) {
        this.intersectionId = intersectionId;
        this.region = region;
        this.timestamp = timestamp;
        vehicleId = signalStatus.getVehicleID();
        vehicleType = signalStatus.getRequesterRole();
        requestId = signalStatus.getRequestID();
        inboundLaneId = signalStatus.getInboundOnLaneID();
        inboundApproachId = signalStatus.getInboundOnApproachID();
        inboundLaneConnectionId = signalStatus.getInboundOnLaneConnectionID();
        outboundLaneId = signalStatus.getOutboundOnLaneID();
        outboundApproachId = signalStatus.getOutboundOnApproachID();
        outboundLaneConnectionId = signalStatus.getOutboundOnLaneConnectionID();
        status = signalStatus.getStatus();
    }

    private String vehicleId;
    private ProcessedBasicVehicleRole vehicleType;
    private String vehicleRole;
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
    private ProcessedPrioritizationResponseStatus status;

    public static final Set<ProcessedPrioritizationResponseStatus> finalStatuses
            = Sets.newHashSet(GRANTED, REJECTED, MAXPRESENCE, RESERVICELOCKED);

    public boolean isFinalStatus() {
        return finalStatuses.contains(status);
    }

}
