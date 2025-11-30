package us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression;

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
@ConfigurationProperties(prefix = "rtcm.message.count.progression")
@ConfigDataClass
public class RtcmMessageCountProgressionParameters {

    @ConfigData(key = "rtcm.message.count.progression.algorithm",
            description = "The algorithm to use",
            updateType = READ_ONLY)
    String algorithm;

    @ConfigData(key = "rtcm.message.count.progression.debug",
            description = "",
            updateType = DEFAULT)
    private volatile boolean debug;

    @ConfigData(key = "rtcm.message.count.progression.rtcmInputTopicName",
            description = "",
            updateType = READ_ONLY)
    private String rtcmInputTopicName;

    @ConfigData(key = "rtcm.message.count.progression.rtcmMessageCountProgressionOutputTopicName",
            description = "",
            updateType = READ_ONLY)
    private String rtcmMessageCountProgressionOutputTopicName;

    @ConfigData(key = "rtcm.message.count.progression.processedRtcmStateStoreName",
            description = "",
            updateType = READ_ONLY)
    private String processedRtcmStateStoreName;

    @ConfigData(key = "rtcm.message.count.progression.latestRtcmStateStoreName",
            description = "",
            updateType = READ_ONLY)
    private String latestRtcmStateStoreName;

    @ConfigData(key = "rtcm.message.count.progression.bufferTimeMs",
            description = "",
            updateType = READ_ONLY)
    private int bufferTimeMs;

    @ConfigData(key = "rtcm.message.count.progression.bufferGracePeriodMs",
            description = "",
            updateType = READ_ONLY)
    private int bufferGracePeriodMs;

    @ConfigData(key = "rtcm.message.count.progression.aggregateEvents",
            description = "",
            updateType = READ_ONLY)
    private boolean aggregateEvents;
}
