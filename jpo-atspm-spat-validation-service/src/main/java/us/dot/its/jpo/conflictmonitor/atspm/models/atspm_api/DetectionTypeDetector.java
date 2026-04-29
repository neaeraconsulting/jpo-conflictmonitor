package us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class DetectionTypeDetector {

    @JsonProperty("ID")
    private int id;

    @JsonProperty("DetectionTypeID")
    private int detectionTypeID;
}

