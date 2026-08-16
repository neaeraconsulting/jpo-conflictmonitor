package us.dot.its.jpo.conflictmonitor.monitor.models.assessments.broadcast_rate;

public class SpatBroadcastRateAssessment extends BroadcastRateAssessment {

    public SpatBroadcastRateAssessment() {
        super("SpatBroadcastRate");
    }

    public boolean isPass() {
        return isPairComparisonPass() && isDurationComparisonPass() && isMaxPairSeparationPass();
    }

}
