package us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class DirectionType {

    @JsonProperty("DirectionTypeID")
    private int directionTypeID;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("Abbreviation")
    private String abbreviation;

    @JsonProperty("DisplayOrder")
    private int displayOrder;
}

