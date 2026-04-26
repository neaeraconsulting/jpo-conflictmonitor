package us.dot.its.jpo.conflictmonitor.atspm.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class LaneType {

    @JsonProperty("LaneTypeID")
    private int laneTypeID;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("Abbreviation")
    private String abbreviation;
}

