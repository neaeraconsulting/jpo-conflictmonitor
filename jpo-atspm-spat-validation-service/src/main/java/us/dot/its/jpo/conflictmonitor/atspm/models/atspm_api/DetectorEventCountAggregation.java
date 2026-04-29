package us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api;

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

