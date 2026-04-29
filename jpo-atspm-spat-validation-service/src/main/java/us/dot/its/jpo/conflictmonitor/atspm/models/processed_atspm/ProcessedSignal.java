package us.dot.its.jpo.conflictmonitor.atspm.models.processed_atspm;

import lombok.Data;
import us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Signal configuration information.  Extracts information from the
 * {@link us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api.Signal}
 * data structure returned from API calls to SignalConfig, and lookup table calls (DirectionType, LaneType,
 * MovementType, etc) into a condensed data structure with information useful for this algorithm
 * to match with MAP and SPAT data.
 */
@Data
public class ProcessedSignal {
    private String signalID;
    private String primaryName;
    private String secondaryName;
    private String latitude;
    private String longitude;
    private String region;
    private String controllerType;
    private boolean enabled;
    private List<ProcessedApproach> approaches;

    public static ProcessedSignal fromSignal(
            Signal signal, List<ControllerType> controllerTypes, List<MovementType> movementTypes,
            List<LaneType> laneTypes, List<DirectionType> directionTypes) {
        Map<Integer, String> controllerTypeMap = controllerTypes.stream().collect(Collectors.toUnmodifiableMap(
                ControllerType::getControllerTypeID,
                ControllerType::getDescription));
        Map<Integer, String> movementTypeMap = movementTypes.stream().collect(Collectors.toUnmodifiableMap(
                MovementType::getMovementTypeID,
                MovementType::getDescription));
        Map<Integer, String> laneTypeMap = laneTypes.stream().collect(Collectors.toUnmodifiableMap(
                LaneType::getLaneTypeID,
                LaneType::getDescription));
        Map<Integer, String> directionTypeMap = directionTypes.stream().collect(Collectors.toUnmodifiableMap(
                DirectionType::getDirectionTypeID,
                DirectionType::getDescription));
        ProcessedSignal ps = new ProcessedSignal();
        ps.setSignalID(signal.getSignalID());
        ps.setPrimaryName(signal.getPrimaryName());
        ps.setSecondaryName(signal.getSecondaryName());
        ps.setLatitude(signal.getLatitude());
        ps.setLongitude(signal.getLongitude());
        ps.setRegion(String.format("%s", signal.getRegion()));
        if (controllerTypeMap.containsKey(signal.getControllerTypeID())) {
            ps.setControllerType(controllerTypeMap.get(signal.getControllerTypeID()));
        }
        ps.setEnabled(signal.isEnabled());
        ps.setApproaches(new ArrayList<>());
        if (signal.getApproaches() != null) {
            for (Approach approach : signal.getApproaches()) {
                ProcessedApproach pa = ProcessedApproach.fromApproach(approach, movementTypeMap, laneTypeMap, directionTypeMap);
                ps.getApproaches().add(pa);
            }
        }
        return ps;
    }
}
