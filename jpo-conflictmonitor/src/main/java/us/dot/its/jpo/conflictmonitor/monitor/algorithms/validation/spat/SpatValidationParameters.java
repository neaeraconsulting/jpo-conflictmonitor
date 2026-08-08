package us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.spat;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UnitsEnum.*;


import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.config.ConfigUtil.*;


import lombok.Data;
import lombok.Generated;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.TimestampType;
import us.dot.its.jpo.conflictmonitor.monitor.models.IntersectionRegion;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigMap;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigData;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigDataClass;


import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UpdateType.*;


@Data
@Generated
@Component
@ConfigurationProperties(prefix = "spat.validation")
@ConfigDataClass
public class SpatValidationParameters {

    @ConfigData(key = "spat.validation.inputTopicName", 
        description = "Input kafka topic", 
        updateType = READ_ONLY)
    String inputTopicName;

    /**
     * Output topic for 'Broadcast Rate' events
     */
    @ConfigData(key = "spat.validation.broadcastRateTopicName", 
        description = "Output topic for Broadcast Rate events", 
        updateType = READ_ONLY)
    String broadcastRateTopicName;

    /**
     * Output topc for 'Minimum Data' events
     */
    @ConfigData(key = "spat.validation.minimumDataTopicName", 
        description = "Output topic for Minimum Data events", 
        updateType = READ_ONLY)
    String minimumDataTopicName;

    // Window parameters
    @ConfigData(key = "spat.validation.rollingPeriodSeconds", 
        units = SECONDS, 
        description = "The aggregation window size for V1 Broadcast Rate",
        updateType = DEFAULT)
    int rollingPeriodSeconds;

    @ConfigData(key = "spat.validation.outputIntervalSeconds", 
        units = SECONDS, 
        description = "The window hop for V1 Broadcast Rate",
        updateType = DEFAULT)
    int outputIntervalSeconds;

    @ConfigData(key = "spat.validation.gracePeriodMilliseconds", 
        units = MILLISECONDS, 
        description = "Window grace period for V1 Broadcast Rate",
        updateType = DEFAULT)
    int gracePeriodMilliseconds;

    // Exclusive min and max to send broadcast rateq events
    @ConfigData(key = "spat.validation.lowerBound", 
        units = PER_PERIOD, 
        description = "Exclusive minimum counts per period for V1 Broadcast Rate",
        updateType = INTERSECTION)
    int lowerBound;

    @ConfigData(key = "spat.validation.upperBound", 
        units = PER_PERIOD, 
        description = "Exclusive maximum counts per period for V1 Broadcast Rate",
        updateType = INTERSECTION)
    int upperBound;

    @ConfigData(key = "spat.validation.v2-broadcast-rate-buffer-size-seconds")
    int v2BroadcastRateBufferSizeSeconds;

    @ConfigData(key = "spat.validation.v2-broadcast-rate-buffer-grace-period-ms")
    int v2BroadcastRateBufferGracePeriodMs;

    @ConfigData(key = "spat.validation.v2-broadcast-rate-lower-bound-pair-separation-ms")
    int v2BroadcastRateLowerBoundPairSeparationMs;

    @ConfigData(key = "spat.validation.v2-broadcast-rate-upper-bound-pair-separation-ms")
    int v2BroadcastRateUpperBoundPairSeparationMs;

    @ConfigData(key = "spat.validation.v2-broadcast-rate-lower-bound-duration-per-10-messages-ms")
    int v2BroadcastRateLowerBoundDurationPer10MessagesMs;

    @ConfigData(key = "spat.validation.v2-broadcast-rate-upper-bound-duration-per-10-messages-ms")
    int v2BroadcastRateUpperBoundDurationPer10MessagesMs;

    @ConfigData(key = "spat.validation.v2-broadcast-rate-conformance-percent-per-hour")
    int v2BroadcastRateConformancePercentPerHour;

    @ConfigData(key = "spat.validation.v2-broadcast-rate-max-outlier-pair-separation-ms")
    int v2BroadcastRateMaxOutlierPairSeparationMs;

    @ConfigData(key = "spat.validation.v2-broadcast-rate-timestamp-type")
    TimestampType v2BroadcastRateTimestampType;

    // Whether to log diagnostic information for debugging
    @ConfigData(key = "spat.validation.debug", 
        description = "Whether to log diagnostic information for debugging", 
        updateType = DEFAULT)
    boolean debug;

    @ConfigData(key = "spat.validation.aggregateEvents",
            description = "Whether to aggregate output minimum data events, or to send each individual event",
            updateType = READ_ONLY)
    boolean aggregateMinimumDataEvents;
   
    
    // Maps for parameters that can be customized per intersection
    final ConfigMap<Integer> lowerBoundMap = new ConfigMap<>();
    final ConfigMap<Integer> upperBoundMap = new ConfigMap<>();

    // Intersection-specific parameters
    public int getLowerBound(IntersectionRegion intersectionKey) {
        return getIntersectionValue(intersectionKey, lowerBoundMap, lowerBound);
    }
    public int getUpperBound(IntersectionRegion intersectionKey) {
        return getIntersectionValue(intersectionKey, upperBoundMap, upperBound);
    }


}
