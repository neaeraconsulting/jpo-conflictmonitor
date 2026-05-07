package us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat;

import lombok.Data;

@Data
public record SignalGroupStatistics (
    int signalGroup,
    double percentGreenPaired,
    double percentYellowPaired,
    double percentRedPaired,
    double percentAllPaired
) {}
