package us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ApproachSplitFailAggregation extends Aggregation {

    @JsonProperty("Id")
    private int id;

    @JsonProperty("ApproachId")
    private int approachId;

    @JsonProperty("SplitFailures")
    private int splitFailures;

    @JsonProperty("IsProtectedPhase")
    private boolean isProtectedPhase;
}

