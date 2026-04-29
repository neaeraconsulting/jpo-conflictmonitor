package us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ApproachPcdAggregation extends Aggregation {

    @JsonProperty("ApproachId")
    private int approachId;

    @JsonProperty("ArrivalsOnGreen")
    private int arrivalsOnGreen;

    @JsonProperty("ArrivalsOnRed")
    private int arrivalsOnRed;

    @JsonProperty("ArrivalsOnYellow")
    private int arrivalsOnYellow;

    @JsonProperty("IsProtectedPhase")
    private boolean isProtectedPhase;

    @JsonProperty("Volume")
    private int volume;
}

