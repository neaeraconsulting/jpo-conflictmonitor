package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import lombok.Data;

import java.time.Instant;
import java.util.List;

@Data
public class ProcessedSpatLog {
    private int routeId;
    private Instant startTime;
    private Instant endTime;
    private List<ProcessedSpatForAtspm> spats;
}
