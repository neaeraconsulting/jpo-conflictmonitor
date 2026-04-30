package us.dot.its.jpo.conflictmonitor.batch.client.atspm;

import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.*;

import java.time.LocalDateTime;
import java.util.List;

public interface AtspmClientService {
    String authenticate();
    String forall();
    Signal signalConfig(String signalId);
    List<ControllerType> controllerType();
    List<DirectionType> directionType();
    List<LaneType> laneType();
    List<MovementType> movementType();
    Approach approachConfig(int approachId);
    Detector detectorConfig(String detectorId);
    List<ControllerEventLog> controllerEventLogs(LocalDateTime startTime, LocalDateTime endTime, int routeId);
}
