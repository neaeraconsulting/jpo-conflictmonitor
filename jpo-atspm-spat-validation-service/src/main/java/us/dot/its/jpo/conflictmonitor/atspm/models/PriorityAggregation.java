package us.dot.its.jpo.conflictmonitor.atspm.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class PriorityAggregation extends Aggregation {

    @JsonProperty("Id")
    private int id;

    @JsonProperty("SignalID")
    private String signalID;

    @JsonProperty("VersionId")
    private int versionId;

    @JsonProperty("PriorityNumber")
    private int priorityNumber;

    @JsonProperty("TotalCycles")
    private int totalCycles;

    @JsonProperty("PriorityRequests")
    private int priorityRequests;

    @JsonProperty("PriorityServiceEarlyGreen")
    private int priorityServiceEarlyGreen;

    @JsonProperty("PriorityServiceExtendedGreen")
    private int priorityServiceExtendedGreen;
}

