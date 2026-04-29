package us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ApproachSpeedAggregation extends Aggregation {

    @JsonProperty("Id")
    private int id;

    @JsonProperty("ApproachId")
    private int approachId;

    @JsonProperty("SummedSpeed")
    private double summedSpeed;

    @JsonProperty("SpeedVolume")
    private double speedVolume;

    @JsonProperty("Speed85Th")
    private double speed85Th;

    @JsonProperty("Speed15Th")
    private double speed15Th;

    @JsonProperty("IsProtectedPhase")
    private boolean isProtectedPhase;
}

