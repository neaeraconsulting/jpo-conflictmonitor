package us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public abstract class Aggregation {

    @JsonProperty("BinStartTime")
    private String binStartTime;
}

