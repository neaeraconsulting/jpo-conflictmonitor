package us.dot.its.jpo.conflictmonitor.monitor.models.assessments.broadcast_rate;

import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.Getter;
import lombok.Setter;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.TimestampType;
import us.dot.its.jpo.conflictmonitor.monitor.models.assessments.Assessment;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;

@Getter
@Setter
@EqualsAndHashCode(callSuper=true)
@Generated
public abstract class BroadcastRateAssessment extends Assessment {

    protected BroadcastRateAssessment(String assessmentType) {
        super(assessmentType);
    }

    protected ProcessingTimePeriod timePeriod;

    protected int numberOfPairViolations;
    protected int numberOfDurationViolations;
    protected int maxPairSeparationMs;
    protected int numberOfSpats;
    private double percentToPass;
    protected int maxAllowedPairSeparationMs;
    protected TimestampType timestampType;


    public void setPercentToPass(double percentToPass) {
        if (percentToPass < 0 || percentToPass > 100.0) {
            throw new IllegalArgumentException("percent must be between 0 and 100.");
        }
        this.percentToPass = percentToPass;
    }

    public double getPercentPairViolations() {
        if (numberOfSpats <= 0) return 0;
        return 100.0 * (double)numberOfPairViolations / (double)numberOfSpats;
    }

    public double getPercentDurationViolations() {
        if (numberOfSpats <= 0) return 0;
        return 100.0 * (double)numberOfDurationViolations / (double)numberOfSpats;
    }

    public boolean isPairComparisonPass() {
        double maxFailPercent = 100.0 - percentToPass;
        return getPercentPairViolations() <= maxFailPercent;
    }

    public boolean isDurationComparisonPass() {
        double maxFailPercent = 100.0 - percentToPass;
        return getPercentDurationViolations() <= maxFailPercent;
    }

    public boolean isMaxPairSeparationPass() {
        return maxPairSeparationMs <= maxAllowedPairSeparationMs;
    }

    public abstract boolean isPass();

}
