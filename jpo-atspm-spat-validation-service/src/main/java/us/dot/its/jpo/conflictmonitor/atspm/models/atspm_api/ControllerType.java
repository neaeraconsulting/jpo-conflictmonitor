package us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ControllerType {

    @JsonProperty("ControllerTypeID")
    private int controllerTypeID;

    @JsonProperty("Description")
    private String description;
}

