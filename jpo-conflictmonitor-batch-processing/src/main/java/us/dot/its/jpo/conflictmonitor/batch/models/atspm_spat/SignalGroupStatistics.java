package us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat;

/**
 * Percent fields are null if there were no transitions of that indication to evaluate,
 * rather than a failing 0% percentage.
 */
public record SignalGroupStatistics (
    int signalGroup,
    Double percentGreenPaired,
    Double percentYellowPaired,
    Double percentRedPaired,
    Double percentAllPaired,
    long greenCount,
    long yellowCount,
    long redCount
) {}
