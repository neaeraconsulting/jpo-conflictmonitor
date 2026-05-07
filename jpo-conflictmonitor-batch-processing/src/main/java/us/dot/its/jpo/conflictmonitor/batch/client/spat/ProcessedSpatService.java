package us.dot.its.jpo.conflictmonitor.batch.client.spat;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatLog;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

import java.time.Instant;
import java.util.List;

public interface ProcessedSpatService {
    List<ProcessedSpat> listProcessedSpats(int intersectionId, Instant startTime, Instant endTime);
    SpatLog spatLogs(int intersectionId, Instant startTime, Instant endTime);
    SignalGroupLog signalGroupLogs(int intersectionId, Instant startTime, Instant endTime);
}
