package us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat;

import lombok.Data;

public record SignalGroupStatistics (
    int signalGroup,
    double percentGreenPaired,
    double percentYellowPaired,
    double percentRedPaired,
    double percentAllPaired,
    /**
     * Number of GREEN/YELLOW/RED SPaT transitions for this signal group that the
     * corresponding percentPaired figure above was computed from. A count of 0 means there
     * was nothing to evaluate for that indication (not that 0% of it paired) - callers
     * should treat percentPaired as not applicable, rather than as a failing percentage,
     * when the matching count is 0.
     */
    long greenCount,
    long yellowCount,
    long redCount
) {}
