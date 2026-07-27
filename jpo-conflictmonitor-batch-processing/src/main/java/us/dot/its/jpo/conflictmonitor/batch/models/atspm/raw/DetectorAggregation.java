package us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class DetectorAggregation {

    @JsonProperty("Id")
    private int id;

    @JsonProperty("BinStartTime")
    private String binStartTime;

    @JsonProperty("DetectorPrimaryId")
    private int detectorPrimaryId;

    @JsonProperty("Volume")
    private int volume;
}

