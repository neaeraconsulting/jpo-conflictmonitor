package us.dot.its.jpo.conflictmonitor.monitor.models.assessments.broadcast_rate;

public class MapBroadcastRateAssessment extends BroadcastRateAssessment {
    public MapBroadcastRateAssessment() {
        super("MapBroadcastRate");
    }

    @Override
    public boolean isPass() {
        // MAPs don't have a single max pair separation criterion
        return isPairComparisonPass() && isDurationComparisonPass();
    }
}
