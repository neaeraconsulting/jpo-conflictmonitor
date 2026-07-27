package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import lombok.Data;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.Detector;

import java.util.Map;

@Data
public class ProcessedDetector {
    private String detectorID;
    private Integer laneNumber;
    private String movementType;
    private String laneType;
    public static ProcessedDetector fromDetector(Detector detector, Map<Integer, String> movementTypeMap,
                                                 Map<Integer, String> laneTypeMap) {
        ProcessedDetector pd = new ProcessedDetector();
        pd.setDetectorID(detector.getDetectorID());
        pd.laneNumber = detector.getLaneNumber();
        if (movementTypeMap.containsKey(detector.getMovementTypeID())) {
            pd.movementType = movementTypeMap.get(detector.getMovementTypeID());
        }
        if (laneTypeMap.containsKey(detector.getLaneTypeID())) {
            pd.laneType = laneTypeMap.get(detector.getLaneTypeID());
        }
        return pd;
    }
}
