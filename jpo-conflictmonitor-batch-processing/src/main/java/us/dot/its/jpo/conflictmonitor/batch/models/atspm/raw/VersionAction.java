package us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class VersionAction {

    @JsonProperty("ID")
    private int id;

    @JsonProperty("Description")
    private String description;
}

