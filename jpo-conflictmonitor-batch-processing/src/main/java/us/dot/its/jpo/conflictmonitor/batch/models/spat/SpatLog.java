package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.List;

/**
 * A time series of SPATs for one intersection
 */
@Document("CmAtspmSpatLog")
@Data
public class SpatLog {
    private int intersectionId;
    private Instant startTime;
    private Instant endTime;
    private List<Spat> spats;
}
