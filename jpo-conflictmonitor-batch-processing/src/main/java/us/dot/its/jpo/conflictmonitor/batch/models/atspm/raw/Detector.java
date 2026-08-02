package us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class Detector {

    @JsonProperty("ID")
    private int id;

    @JsonProperty("DetectorID")
    private String detectorID;

    @JsonProperty("DetChannel")
    private int detChannel;

    @JsonProperty("DistanceFromStopBar")
    private Integer distanceFromStopBar;

    @JsonProperty("MinSpeedFilter")
    private Integer minSpeedFilter;

    @JsonProperty("DateAdded")
    private String dateAdded;

    @JsonProperty("DateDisabled")
    private String dateDisabled;

    @JsonProperty("DetectionTypeIDs")
    private List<Integer> detectionTypeIDs;

    @JsonProperty("DetectionTypes")
    private List<DetectionType> detectionTypes;

    @JsonProperty("LaneNumber")
    private Integer laneNumber;

    @JsonProperty("MovementTypeID")
    private Integer movementTypeID;

    @JsonProperty("MovementType")
    private MovementType movementType;

    @JsonProperty("LaneTypeID")
    private Integer laneTypeID;

    @JsonProperty("LaneType")
    private LaneType laneType;

    @JsonProperty("DecisionPoint")
    private Integer decisionPoint;

    @JsonProperty("MovementDelay")
    private Integer movementDelay;

    @JsonProperty("LatencyCorrection")
    private double latencyCorrection;

    @JsonProperty("DetectorCommentIDs")
    private List<Integer> detectorCommentIDs;

    @JsonProperty("ApproachID")
    private int approachID;

    @JsonProperty("Approach")
    private Approach approach;

    @JsonProperty("DetectionHardwareID")
    private int detectionHardwareID;

    @JsonProperty("DetectionHardware")
    private DetectionHardware detectionHardware;
}

