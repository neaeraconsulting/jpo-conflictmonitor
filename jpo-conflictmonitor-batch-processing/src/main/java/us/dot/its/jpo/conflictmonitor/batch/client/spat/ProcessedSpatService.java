package us.dot.its.jpo.conflictmonitor.batch.client.spat;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

import java.time.Instant;
import java.util.List;

public interface ProcessedSpatService {
    List<ProcessedSpat> findByIntersectionIdAndTimestamp(
            int intersectionId, Instant startTime, Instant endTime);
}
