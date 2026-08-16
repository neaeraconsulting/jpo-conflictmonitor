package us.dot.its.jpo.conflictmonitor.monitor.models.assessments.broadcast_rate;

public class MapBroadcastRateAssessment extends BroadcastRateAssessment {
    public MapBroadcastRateAssessment() {
        super("MapBroadcastRate");
    }

    @Override
    public boolean isPass() {
        // Map doesn't have a single max pair separation criterion
        return isPairComparisonPass() && isDurationComparisonPass();
    }
}
