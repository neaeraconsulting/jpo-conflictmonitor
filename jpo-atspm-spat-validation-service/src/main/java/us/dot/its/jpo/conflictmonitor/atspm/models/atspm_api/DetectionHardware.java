package us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class DetectionHardware {

    @JsonProperty("ID")
    private int id;

    @JsonProperty("Name")
    private String name;
}

