package us.dot.its.jpo.conflictmonitor.monitor.models.assessments.broadcast_rate;

import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.Getter;
import lombok.Setter;
import us.dot.its.jpo.conflictmonitor.monitor.models.assessments.Assessment;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;

@Getter
@Setter
@EqualsAndHashCode(callSuper=true)
@Generated
public abstract class BroadcastRateAssessment extends Assessment {

    public BroadcastRateAssessment(String assessmentType) {
        super(assessmentType);
    }

    protected ProcessingTimePeriod timePeriod;

    protected int numberOfPairViolations;
    protected int numberOfDurationViolations;
    protected int maxPairSeparationMs;
    protected int numberOfSpats;
    private double percentToPass;
    protected int maxAllowedPairSeparationMs;

    public void setPercentToPass(double percentToPass) {
        if (percentToPass < 0 || percentToPass > 100.0) {
            throw new IllegalArgumentException("percent must be between 0 and 100.");
        }
        this.percentToPass = percentToPass;
    }

    public double getPercentPairViolations() {
        int numberOfPairs = numberOfSpats - 1;
        if (numberOfPairs <= 0) return 0;
        return 100.0 * (double)numberOfPairViolations / (double)numberOfPairs;
    }

    public double getPercentDurationViolations() {
        int numberOfDurations = numberOfSpats - 10;
        if (numberOfDurations <= 0) return 0;
        return 100.0 * (double)numberOfDurationViolations / (double)numberOfDurations;
    }

    public boolean isPass() {
        double maxFailPercent = 100.0 - percentToPass;
        if (getPercentPairViolations() > maxFailPercent) return false;
        if (getPercentDurationViolations() > maxFailPercent) return false;
        if (maxPairSeparationMs > maxAllowedPairSeparationMs) return false;
        return true;
    }

}
