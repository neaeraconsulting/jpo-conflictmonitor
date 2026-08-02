package us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ApproachEventCountAggregation extends Aggregation {

    @JsonProperty("ApproachId")
    private int approachId;

    @JsonProperty("EventCount")
    private int eventCount;

    @JsonProperty("IsProtectedPhase")
    private boolean isProtectedPhase;
}

