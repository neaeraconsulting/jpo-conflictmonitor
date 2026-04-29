package us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class PhaseTerminationAggregation extends Aggregation {

    @JsonProperty("Id")
    private int id;

    @JsonProperty("SignalId")
    private String signalId;

    @JsonProperty("PhaseNumber")
    private int phaseNumber;

    @JsonProperty("GapOuts")
    private int gapOuts;

    @JsonProperty("ForceOffs")
    private int forceOffs;

    @JsonProperty("MaxOuts")
    private int maxOuts;

    @JsonProperty("UnknownTerminationTypes")
    private int unknownTerminationTypes;
}

