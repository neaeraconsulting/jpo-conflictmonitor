package us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression;

import lombok.Data;
import lombok.Generated;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigData;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigDataClass;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.UnitsEnum;

import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UnitsEnum.MILLISECONDS;
import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UpdateType.DEFAULT;
import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UpdateType.READ_ONLY;

/**
 * Parameters for the RTCM Message Count Progression algorithm
 */
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
            description = "Whether to log diagnostic information for debugging",
            updateType = DEFAULT)
    private volatile boolean debug;

    @ConfigData(key = "rtcm.message.count.progression.rtcmInputTopicName",
            description = "The name of the topic to read ProcessedRTCM messages from",
            updateType = READ_ONLY)
    private String rtcmInputTopicName;

    @ConfigData(key = "rtcm.message.count.progression.rtcmMessageCountProgressionOutputTopicName",
            description = "The name of the topic to write RTCM Message Count Progression events to",
            updateType = READ_ONLY)
    private String rtcmMessageCountProgressionOutputTopicName;

    @ConfigData(key = "rtcm.message.count.progression.processedRtcmStateStoreName",
            description = "Name of the versioned state store for the jitter buffer",
            updateType = READ_ONLY)
    private String processedRtcmStateStoreName;

    @ConfigData(key = "rtcm.message.count.progression.latestRtcmStateStoreName",
            description = "Name of key-value store to keep track of the latest BSM",
            updateType = READ_ONLY)
    private String latestRtcmStateStoreName;

    @ConfigData(key = "rtcm.message.count.progression.latestEventStateStoreName",
        description = "Name of key-value store to keep track of the latest event sent per key to prevent duplicate events",
        updateType = READ_ONLY)
    private String latestEventStateStoreName;

    @ConfigData(key = "rtcm.message.count.progression.bufferTimeMs",
            description = "The size of the RTCM buffer. Must be larger than the expected interval between RTCMS plus expected jitter time.",
            updateType = READ_ONLY,
            units = MILLISECONDS)
    private int bufferTimeMs;

    @ConfigData(key = "rtcm.message.count.progression.bufferGracePeriodMs",
            description = "The grace period to allow late out-of-order BSMs to arrive before checking for transition events. Must be less than bufferTimeMs.",
            updateType = READ_ONLY,
            units = MILLISECONDS)
    private int bufferGracePeriodMs;

    @ConfigData(key = "rtcm.message.count.progression.aggregateEvents",
            description = "Whether to aggregate the RTCM Message Count Progression aggregation events",
            updateType = READ_ONLY)
    private boolean aggregateEvents;

    @ConfigData(key = "rtcm.message.count.progression.checkIntervalMs",
        description = "Clock time interval to check whether to emit events in the case where stream time doesn't advance",
        updateType = READ_ONLY,
        units = MILLISECONDS)
    private int checkIntervalMs;
}
