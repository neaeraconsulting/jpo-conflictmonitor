package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import lombok.Data;

import java.util.Set;

@Data
public class Lane {
    private int laneNumber;
    private String laneType;
    private Set<String> movements;
}
