package us.dot.its.jpo.conflictmonitor.atspm.models.processed_atspm;

import lombok.Data;

import java.time.Instant;

@Data
public class ProcessedControllerEvent {
    private String signalId;
    private Instant timestamp;
    private EventCode eventCode;
    private int phase;
}
