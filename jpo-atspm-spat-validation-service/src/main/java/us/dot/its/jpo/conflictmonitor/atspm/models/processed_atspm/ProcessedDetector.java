package us.dot.its.jpo.conflictmonitor.atspm.models.processed_atspm;

import lombok.Data;

@Data
public class ProcessedDetector {
    private String detectorID;
    private Integer laneNumber;
    private String movementType;
    private String laneType;
}
