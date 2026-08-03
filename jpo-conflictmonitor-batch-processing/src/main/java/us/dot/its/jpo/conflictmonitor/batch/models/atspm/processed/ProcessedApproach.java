package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import lombok.Data;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.Approach;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.Detector;

import java.util.*;

@Data
public class ProcessedApproach {
    private int approachID;
    private String directionType;
    private String description;
    private Integer protectedPhaseNumber;
    private Integer permissivePhaseNumber;
    private Integer pedestrianPhaseNumber;
    private Set<Lane> lanes;

    public static ProcessedApproach fromApproach(
            Approach approach, Map<Integer, String> movementTypeMap,
            Map<Integer, String> laneTypeMap, Map<Integer, String> directionTypeMap) {
        ProcessedApproach pa = new ProcessedApproach();
        pa.approachID = approach.getApproachID();
        if (directionTypeMap.containsKey(approach.getDirectionTypeID())) {
            pa.setDirectionType(directionTypeMap.get(approach.getDirectionTypeID()));
        }
        pa.description = approach.getDescription();
        pa.protectedPhaseNumber = approach.getProtectedPhaseNumber();
        pa.permissivePhaseNumber = approach.getPermissivePhaseNumber();
        pa.pedestrianPhaseNumber = approach.getPedestrianPhaseNumber();
        pa.lanes = new LinkedHashSet<>();
        if (approach.getDetectors() != null) {
            for (Detector detector : approach.getDetectors()) {
                ProcessedDetector pd = ProcessedDetector.fromDetector(detector, movementTypeMap, laneTypeMap);
                final Lane lane = new Lane();
                lane.setLaneNumber(pd.getLaneNumber());
                lane.setLaneType(pd.getLaneType());
                lane.setMovements(new HashSet<>());
                lane.getMovements().add(pd.getMovementType());
                Lane existingLane = pa.getLanes().stream().filter(l -> l.getLaneNumber() == lane.getLaneNumber()).findFirst().orElse(null);
                if (existingLane != null) {
                    existingLane.getMovements().add(pd.getMovementType());
                } else {
                    pa.getLanes().add(lane);
                }
            }
        }

        return pa;
    }
}
