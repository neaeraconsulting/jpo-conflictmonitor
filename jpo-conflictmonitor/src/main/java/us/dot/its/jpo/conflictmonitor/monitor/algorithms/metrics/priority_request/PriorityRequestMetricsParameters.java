package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request;

import lombok.Data;
import lombok.Generated;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigData;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigDataClass;

import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UpdateType.DEFAULT;
import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UpdateType.READ_ONLY;

@Data
@Generated
@Component
@ConfigurationProperties(prefix = "metrics.priority.request")
@ConfigDataClass
public class PriorityRequestMetricsParameters {

    @ConfigData(key = "metrics.priority.request.debug",
            description = "Whether to log diagnostic information for debugging",
            updateType = DEFAULT)
    private volatile boolean debug;

    @ConfigData(key = "metrics.priority.request.inputEventTopic",
            description = "Input topic with Priority/Preemption Request Events",
            updateType = READ_ONLY)
    private String inputEventTopic;

    @ConfigData(key = "metrics.priority.request.inputSrmTopic",
            description = "Input topic with ProcessedSrms",
            updateType = READ_ONLY)
    private String inputSrmTopic;

    @ConfigData(key = "metrics.priority.request.inputSsmTopic",
            description = "Input topic with ProcessedSsms",
            updateType = READ_ONLY)
    private String inputSsmTopic;

    @ConfigData(key = "metrics.priority.request.outputTopic",
            description = "Output topic for metrics",
            updateType = READ_ONLY)
    private String outputTopic;

}
