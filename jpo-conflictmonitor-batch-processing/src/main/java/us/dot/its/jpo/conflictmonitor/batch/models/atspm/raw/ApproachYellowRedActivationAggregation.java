package us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ApproachYellowRedActivationAggregation extends Aggregation {

    @JsonProperty("ApproachId")
    private int approachId;

    @JsonProperty("SevereRedLightViolations")
    private int severeRedLightViolations;

    @JsonProperty("TotalRedLightViolations")
    private int totalRedLightViolations;

    @JsonProperty("IsProtectedPhase")
    private boolean isProtectedPhase;
}

