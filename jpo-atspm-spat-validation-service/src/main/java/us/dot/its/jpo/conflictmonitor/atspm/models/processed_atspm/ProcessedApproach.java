package us.dot.its.jpo.conflictmonitor.atspm.models.processed_atspm;

import lombok.Data;
import us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class ProcessedApproach {
    private int approachID;
    private String directionType;
    private String description;
    private Integer protectedPhaseNumber;
    private Integer permissivePhaseNumber;
    private Integer pedestrianPhaseNumber;
    private List<ProcessedDetector> detectors;
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
        pa.detectors = new ArrayList<>();
        if (approach.getDetectors() != null) {
            for (Detector detector : approach.getDetectors()) {
                ProcessedDetector pd = ProcessedDetector.fromDetector(detector, movementTypeMap, laneTypeMap);
                pa.getDetectors().add(pd);
            }
        }
        return pa;
    }
}
