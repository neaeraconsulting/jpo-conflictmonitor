package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import lombok.Data;

import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Signal configuration information.  Extracts information from the
 * {@link Signal}
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
        ps.signalID = signal.getSignalID();
        ps.primaryName = signal.getPrimaryName();
        ps.secondaryName = signal.getSecondaryName();
        ps.latitude = signal.getLatitude();
        ps.longitude = signal.getLongitude();
        ps.region = String.format("%s", signal.getRegion());
        if (controllerTypeMap.containsKey(signal.getControllerTypeID())) {
            ps.controllerType = controllerTypeMap.get(signal.getControllerTypeID());
        }
        ps.enabled = signal.isEnabled();
        ps.approaches = new ArrayList<>();
        if (signal.getApproaches() != null) {
            for (Approach approach : signal.getApproaches()) {
                ProcessedApproach pa = ProcessedApproach.fromApproach(approach, movementTypeMap, laneTypeMap, directionTypeMap);
                ps.getApproaches().add(pa);
            }
        }
        return ps;
    }
}
