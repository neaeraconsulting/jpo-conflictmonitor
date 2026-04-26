package us.dot.its.jpo.conflictmonitor.atspm.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class Signal {

    @JsonProperty("VersionID")
    private int versionID;

    @JsonProperty("SignalID")
    private String signalID;

    @JsonProperty("VersionActionId")
    private int versionActionId;

    @JsonProperty("VersionAction")
    private VersionAction versionAction;

    @JsonProperty("Note")
    private String note;

    @JsonProperty("Start")
    private String start;

    @JsonProperty("PrimaryName")
    private String primaryName;

    @JsonProperty("SecondaryName")
    private String secondaryName;

    @JsonProperty("Latitude")
    private String latitude;

    @JsonProperty("Longitude")
    private String longitude;

    @JsonProperty("RegionID")
    private int regionID;

    @JsonProperty("Region")
    private Region region;

    @JsonProperty("ControllerTypeID")
    private int controllerTypeID;

    @JsonProperty("Enabled")
    private boolean enabled;

    @JsonProperty("Pedsare1to1")
    private boolean pedsare1to1;

    @JsonProperty("Approaches")
    private List<Approach> approaches;
}

