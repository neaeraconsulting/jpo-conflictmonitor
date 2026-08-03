package us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ControllerType {

    @JsonProperty("ControllerTypeID")
    private int controllerTypeID;

    @JsonProperty("Description")
    private String description;
}

