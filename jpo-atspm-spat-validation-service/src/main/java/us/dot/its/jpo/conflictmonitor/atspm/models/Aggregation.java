package us.dot.its.jpo.conflictmonitor.atspm.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public abstract class Aggregation {

    @JsonProperty("BinStartTime")
    private String binStartTime;
}

