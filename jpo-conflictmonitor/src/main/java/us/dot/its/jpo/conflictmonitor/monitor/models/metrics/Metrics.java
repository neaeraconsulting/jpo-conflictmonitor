package us.dot.its.jpo.conflictmonitor.monitor.models.metrics;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.Generated;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;

import java.util.Map;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "name"
)
@JsonSubTypes({
        @Type(value = PriorityRequestMetrics.class,
                name = "PriorityRequest"),
        @Type(value = DynamicLaneActivationMetrics.class,
            name = "DynamicLaneActivationMetric")
})
@Data
@Generated
public abstract class Metrics<TKey> {

    protected TKey key;
    protected final String name;
    protected final long metricGeneratedAt;
    protected ProcessingTimePeriod timePeriod;

    protected Metrics(String name) {
        this.name = name;
        this.metricGeneratedAt = System.currentTimeMillis();
    }
}
