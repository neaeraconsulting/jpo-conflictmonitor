package us.dot.its.jpo.conflictmonitor.atspm.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ControllerEventLog {

    @JsonProperty("SignalID")
    private String signalId;

    @JsonProperty("Timestamp")
    private String timestamp;

    @JsonProperty("EventCode")
    private int eventCode;

    @JsonProperty("EventParam")
    private int eventParam;
}
