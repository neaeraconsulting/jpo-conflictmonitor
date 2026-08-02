package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation;

import lombok.Data;
import lombok.Generated;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigDataClass;

@Data
@Generated
@Component
@ConfigurationProperties(prefix = "metrics.dynamic.lane.activation")
@ConfigDataClass
public class DynamicLaneActivationMetricsParameters {
    private String algorithm;
    private volatile boolean debug;
    private String outputMetricTopic;
}
