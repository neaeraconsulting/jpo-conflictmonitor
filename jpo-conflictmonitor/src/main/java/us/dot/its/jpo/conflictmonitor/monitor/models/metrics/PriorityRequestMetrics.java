package us.dot.its.jpo.conflictmonitor.monitor.models.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.IntersectionVehicleRequestKey;

import java.util.HashSet;
import java.util.Set;

/**
 * Performance and Operation Metrics: Priority request fulfillment rate.
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class PriorityRequestMetrics
        extends Metrics<IntersectionVehicleTypeKey> {

    /**
     * @return Top level intersection id needed for Mongo collection
     */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    public Integer getIntersectionID() {
        return key != null ? key.getIntersectionId() : null;
    }

    /**
     * The number distinct request IDs in valid SRMs for one intersection and vehicle type
     * during the time period. Typically, vehicles send multiple SRMs before receiving an SSM response.
     * Distinct SRM Request IDs are only counted once.
     */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    public long getNumberOfDistinctSrmRequests() {
        return distinctSrmRequestKeys.size();
    }

    @NotNull
    private Set<IntersectionVehicleRequestKey> distinctSrmRequestKeys = new HashSet<>();

    public void addDistinctSrmRequestKeys(IntersectionVehicleRequestKey key) {
        distinctSrmRequestKeys.add(key);
    }

    /**
     * The number of SSM "granted" responses for the intersection and vehicle type during the
     * time period.
     */
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    public long getNumberOfGrantedSsmResponses() {
        return distinctSsmResponseKeys.size();
    }

    @NotNull
    private Set<IntersectionVehicleRequestKey> distinctSsmResponseKeys = new HashSet<>();

    public void addDistinctSsmResponseKeys(IntersectionVehicleRequestKey key) {
        distinctSsmResponseKeys.add(key);
    }

    /**
     * @return The priority request fulfillment rate.  Fraction of unique requests fulfilled with a final
     * status of "granted", or null if there were no requests.
     */
    public Double getFulfillmentRate() {
        if (getNumberOfDistinctSrmRequests() == 0) return null;
        return (double) getNumberOfGrantedSsmResponses() / (double)getNumberOfDistinctSrmRequests();
    }

    public PriorityRequestMetrics() {
        super("PriorityRequest");
    }


}
