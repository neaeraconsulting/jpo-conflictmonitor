package us.dot.its.jpo.conflictmonitor.monitor.models.metrics;

import lombok.*;


@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class PriorityRequestMetrics
        extends Metrics<IntersectionVehicleTypeKey> {

    /**
     * The number distinct request IDs in valid SRMs for one intersection and vehicle type
     * during the time period. Typically, vehicles send multiple SRMs before receiving an SSM response.
     * Distinct SRM Request IDs are only counted once.
     */
    private long numberOfDistinctSrmRequests;

    /**
     * The number of SSM "granted" responses for the intersection and vehicle type during the
     * time period.
     */
    private long numberOfGrantedSsmResponses;

    public double getFulfillmentRate() {
        if (numberOfDistinctSrmRequests == 0) return 0;
        return (double) numberOfGrantedSsmResponses / (double)numberOfDistinctSrmRequests;
    }

    public PriorityRequestMetrics() {
        super("PriorityRequest");
    }


}
