package us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class PreemptionAggregation extends Aggregation {

    @JsonProperty("Id")
    private int id;

    @JsonProperty("SignalId")
    private String signalId;

    @JsonProperty("VersionId")
    private int versionId;

    @JsonProperty("PreemptNumber")
    private int preemptNumber;

    @JsonProperty("PreemptRequests")
    private int preemptRequests;

    @JsonProperty("PreemptServices")
    private int preemptServices;
}

