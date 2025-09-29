package us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request;

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
@ConfigurationProperties(prefix = "priority.preemption.request")
@ConfigDataClass
public class PriorityPreemptionRequestParameters {

    @ConfigData(key = "priority.preemption.request.debug",
            description = "Whether to log diagnostic information for debugging",
            updateType = DEFAULT)
    private volatile boolean debug;

    @ConfigData(key = "priority.preemption.request.algorithm",
            description = "",
            updateType = READ_ONLY)
    private String algorithm;

    @ConfigData(key = "priority.preemption.request.processedSrmInputTopic", description = "", updateType = READ_ONLY)
    private String processedSrmInputTopic;

    @ConfigData(key = "priority.preemption.request.processedSsmInputTopic", description = "", updateType = READ_ONLY)
    private String processedSsmInputTopic;

    @ConfigData(key = "priority.preemption.request.outputEventTopic", description = "", updateType = READ_ONLY)
    private String outputEventTopic;

}
