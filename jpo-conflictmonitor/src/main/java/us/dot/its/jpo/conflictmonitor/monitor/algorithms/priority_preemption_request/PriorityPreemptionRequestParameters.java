package us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request;

import lombok.Data;
import lombok.Generated;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigData;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigDataClass;

import java.time.temporal.ChronoUnit;

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
            updateType = READ_ONLY)
    private boolean debug;

    @ConfigData(key = "priority.preemption.request.algorithm",
            description = "",
            updateType = READ_ONLY)
    private String algorithm;

    @ConfigData(key = "priority.preemption.request.processedSrmInputTopic",
            description = "",
            updateType = READ_ONLY)
    private String processedSrmInputTopic;

    @ConfigData(key = "priority.preemption.request.processedSsmInputTopic",
            description = "",
            updateType = READ_ONLY)
    private String processedSsmInputTopic;

    @ConfigData(key = "priority.preemption.request.outputEventTopic",
            description = "",
            updateType = READ_ONLY)
    private String outputEventTopic;

    @ConfigData(key = "priority.preemption.request.srmStoreName",
            description = "Name of the versioned state store to hold SRM Requests",
            updateType = READ_ONLY)
    private String srmStoreName;

    @ConfigData(key = "priority.preemption.request.ssmStoreName",
            description = "Name of the versioned state store to hold SSM responses",
            updateType = READ_ONLY)
    private String ssmStoreName;

    @ConfigData(key = "priority.preemption.request.storeRetentionTime",
            description = "Retention time of the versioned store stores",
            updateType = READ_ONLY)
    private int storeRetentionTime;

    @ConfigData(key = "priority.preemption.request.retentionTimeUnits",
            description = "Time units of the SSM and SRM store retention time parameters",
            updateType = READ_ONLY)
    private ChronoUnit retentionTimeUnits;

    @ConfigData(key = "priority.preemption.request.maxTimeBetweenSrms",
            description = """
                The maximum time between SRM messages with the same IntersectionVehcleRequestKey for them to be
                considered part of the same request. If an SRM with the given key is not received for longer than this
                time since the last one, and no matching SSM has is received, a Priority/Preemption Request Event is 
                emitted indicating the SRM did not receive a matching SSM.""",
            updateType = READ_ONLY)
    private int maxTimeBetweenSrms;

    @ConfigData(key = "priority.preemption.request.maxTimeBetweenSrmsUnits",
            description = "Time units of the max time between SRMs parameter",
            updateType = READ_ONLY)
    private ChronoUnit maxTimeBetweenSrmsUnits;

}
