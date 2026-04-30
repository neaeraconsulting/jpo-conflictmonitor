package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import lombok.Data;

import java.time.Instant;

@Data
public class ProcessedControllerEvent {
    private String signalId;
    private Instant timestamp;
    private EventCode eventCode;
    private int phase;
}
