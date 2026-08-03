package us.dot.its.jpo.conflictmonitor.batch.services.atspm_spat_validation;

import us.dot.its.jpo.conflictmonitor.batch.events.AtspmSpatSignalGroupAlignmentEvent;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;

import java.time.Instant;
import java.util.List;

public interface AtspmSpatValidationService {
    List<AtspmSpatPairLog> atspmSpatLogs(int routeId, Instant startTime, Instant endTime);
    List<AtspmSpatSignalGroupAlignmentEvent> atspmSpatSignalGroupAlignmentEvents(int routeId, Instant startTime, Instant endTime);

}
