package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

import lombok.Data;

@Data
public class SignalConfig {
    private String signalId;
    private String description;
    private boolean enabled;
}
