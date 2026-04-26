package us.dot.its.jpo.conflictmonitor.atspm.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class Approach {

    @JsonProperty("ApproachID")
    private int approachID;

    @JsonProperty("SignalID")
    private String signalID;

    @JsonProperty("VersionID")
    private int versionID;

    @JsonProperty("Signal")
    private Signal signal;

    @JsonProperty("DirectionTypeID")
    private int directionTypeID;

    @JsonProperty("DirectionType")
    private DirectionType directionType;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("MPH")
    private Integer mph;

    @JsonProperty("ProtectedPhaseNumber")
    private int protectedPhaseNumber;

    @JsonProperty("IsProtectedPhaseOverlap")
    private boolean isProtectedPhaseOverlap;

    @JsonProperty("PermissivePhaseNumber")
    private Integer permissivePhaseNumber;

    @JsonProperty("IsPermissivePhaseOverlap")
    private boolean isPermissivePhaseOverlap;

    @JsonProperty("PedestrianPhaseNumber")
    private Integer pedestrianPhaseNumber;

    @JsonProperty("IsPedestrianPhaseOverlap")
    private boolean isPedestrianPhaseOverlap;

    @JsonProperty("PedestrianDetectors")
    private String pedestrianDetectors;

    @JsonProperty("Detectors")
    private List<Detector> detectors;
}

