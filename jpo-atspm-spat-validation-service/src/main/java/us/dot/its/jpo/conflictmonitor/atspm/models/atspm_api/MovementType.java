package us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class MovementType {

    @JsonProperty("MovementTypeID")
    private int movementTypeID;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("Abbreviation")
    private String abbreviation;

    @JsonProperty("DisplayOrder")
    private int displayOrder;
}

