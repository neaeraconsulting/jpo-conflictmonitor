package us.dot.its.jpo.conflictmonitor.monitor.models.assessments.broadcast_rate;

import lombok.EqualsAndHashCode;
import lombok.Generated;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(callSuper=true)
@Generated
public class SpatBroadcastRateAssessment extends BroadcastRateAssessment {

    public SpatBroadcastRateAssessment() {
        super("SpatBroadcastRate");
    }

    protected int maxAllowedPairSeparationMs;

    public boolean isMaxPairSeparationPass() {
        return maxPairSeparationMs <= maxAllowedPairSeparationMs;
    }

    public boolean isPassWithMaxPairSeparation() {
        return isPairComparisonPass() && isDurationComparisonPass() && isMaxPairSeparationPass();
    }

}
