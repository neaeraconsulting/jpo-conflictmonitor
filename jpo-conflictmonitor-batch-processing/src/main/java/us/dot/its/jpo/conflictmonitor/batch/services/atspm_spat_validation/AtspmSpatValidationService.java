package us.dot.its.jpo.conflictmonitor.batch.services.atspm_spat_validation;

import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;

import java.time.Instant;
import java.util.List;

public interface AtspmSpatValidationService {
    List<AtspmSpatPairLog> atpsmSpatLogs(int routeId, Instant startTime, Instant endTime);
}
