package us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class DetectorEventCountAggregation {

    @JsonProperty("BinStartTime")
    private String binStartTime;

    @JsonProperty("DetectorPrimaryId")
    private int detectorPrimaryId;

    @JsonProperty("EventCount")
    private int eventCount;
}

