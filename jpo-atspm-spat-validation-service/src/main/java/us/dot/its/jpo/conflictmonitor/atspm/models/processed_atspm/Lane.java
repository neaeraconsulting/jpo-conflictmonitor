package us.dot.its.jpo.conflictmonitor.atspm.models.processed_atspm;

import lombok.Data;

import java.util.Set;

@Data
public class Lane {
    private int laneNumber;
    private String laneType;
    private Set<String> movements;
}
