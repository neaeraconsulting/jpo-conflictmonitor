package us.dot.its.jpo.conflictmonitor.batch.services.spat;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupIndicationLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupStateLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatLog;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

import java.time.Instant;
import java.util.List;

public interface ProcessedSpatService {
    List<ProcessedSpat> listProcessedSpats(int intersectionId, Instant startTime, Instant endTime);
    SpatLog spatLogs(int intersectionId, Instant startTime, Instant endTime);
    SignalGroupStateLog signalGroupLogs(int intersectionId, Instant startTime, Instant endTime);
    SignalGroupIndicationLog signalGroupIndicationLogs(int intersectionId, Instant startTime, Instant endTime);
}
