package us.dot.its.jpo.conflictmonitor.monitor.models.metrics;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Data;
import lombok.Generated;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation.DynamicLaneActivationMetrics;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;

/**
 * Performance and Operational Metrics.  SDD July 2025, section 3.5.27
 * @param <TKey> The type of the key to aggregate the metrics on.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "name"
)
@JsonSubTypes({
        @Type(value = PriorityRequestMetrics.class,
                name = "PriorityRequest"),
        @Type(value = DynamicLaneActivationMetrics.class,
            name = "DynamicLaneActivation")
})
@Data
@Generated
@Slf4j
public abstract class Metrics<TKey> {

    protected TKey key;
    protected final String name;
    protected final long metricGeneratedAt;
    protected ProcessingTimePeriod timePeriod;

    protected Metrics(String name) {
        this.name = name;
        this.metricGeneratedAt = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        try {
            return DateJsonMapper.getInstance().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            log.error("Exception serializing {} Metrics to JSON", name, e);
        }
        return "";
    }
}
