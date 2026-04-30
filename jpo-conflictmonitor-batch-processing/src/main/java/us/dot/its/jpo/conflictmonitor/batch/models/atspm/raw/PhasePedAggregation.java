package us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class PhasePedAggregation extends Aggregation {

    @JsonProperty("Id")
    private int id;

    @JsonProperty("SignalId")
    private String signalId;

    @JsonProperty("PhaseNumber")
    private int phaseNumber;

    @JsonProperty("PedCount")
    private int pedCount;

    @JsonProperty("PedDelay")
    private double pedDelay;
}

