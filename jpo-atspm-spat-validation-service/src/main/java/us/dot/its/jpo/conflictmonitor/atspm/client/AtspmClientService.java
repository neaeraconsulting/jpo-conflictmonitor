package us.dot.its.jpo.conflictmonitor.atspm.client;

import us.dot.its.jpo.conflictmonitor.atspm.models.ControllerEventLog;
import us.dot.its.jpo.conflictmonitor.atspm.models.ControllerType;
import us.dot.its.jpo.conflictmonitor.atspm.models.Signal;


import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

public interface AtspmClientService {
    String authenticate();
    String forall();
    List<Signal> signalConfig(String signalId);
    List<ControllerType> controllerType();
    List<ControllerEventLog> controllerEventLogs(LocalDateTime startTime, LocalDateTime endTime, int routeId);
}
