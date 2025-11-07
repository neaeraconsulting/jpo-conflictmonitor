package us.dot.its.jpo.conflictmonitor.monitor.models.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

/**
 * Performance and Operation Metrics: Priority request fulfillment rate.
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class PriorityRequestMetrics
        extends Metrics<IntersectionVehicleTypeKey> {

    /**
     * @return Top level intersection id needed form Mongo collection
     */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    public Integer getIntersectionID() {
        return key.getIntersectionId() != null ? key.getIntersectionId() : null;
    }

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

    /**
     * @return The priority request fulfillment rate.  Fraction of unique requests fulfilled with a final
     * status of "granted", or null if there were no requests.
     */
    public Double getFulfillmentRate() {
        if (numberOfDistinctSrmRequests == 0) return null;
        return (double) numberOfGrantedSsmResponses / (double)numberOfDistinctSrmRequests;
    }

    public PriorityRequestMetrics() {
        super("PriorityRequest");
    }


}
