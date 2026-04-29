package us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ApproachCycleAggregation extends Aggregation {

    @JsonProperty("Id")
    private int id;

    @JsonProperty("ApproachId")
    private int approachId;

    @JsonProperty("RedTime")
    private double redTime;

    @JsonProperty("YellowTime")
    private double yellowTime;

    @JsonProperty("GreenTime")
    private double greenTime;

    @JsonProperty("TotalCycles")
    private int totalCycles;

    @JsonProperty("PedActuations")
    private int pedActuations;

    @JsonProperty("IsProtectedPhase")
    private boolean isProtectedPhase;
}

