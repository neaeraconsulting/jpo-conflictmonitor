package us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class SpeedEvents {

    @JsonProperty("DetectorID")
    private String detectorID;

    @JsonProperty("MPH")
    private int mph;

    @JsonProperty("KPH")
    private int kph;

    @JsonProperty("timestamp")
    private String timestamp;
}

