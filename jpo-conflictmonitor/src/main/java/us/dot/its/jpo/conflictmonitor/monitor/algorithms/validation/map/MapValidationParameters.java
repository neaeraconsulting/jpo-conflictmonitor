package us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.map;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.config.ConfigUtil.getIntersectionValue;
import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UnitsEnum.MILLISECONDS;
import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UnitsEnum.PER_PERIOD;
import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UnitsEnum.SECONDS;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;
import lombok.Generated;
import us.dot.its.jpo.conflictmonitor.monitor.models.IntersectionRegion;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigMap;
import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UpdateType.*;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigData;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigDataClass;

import java.time.temporal.ChronoUnit;


@Data
@Generated
@Component
@ConfigurationProperties(prefix = "map.validation")
@ConfigDataClass
public class MapValidationParameters {
  

    @ConfigData(key = "map.validation.inputTopicName", 
        description = "Input kafka topic", 
        updateType = READ_ONLY)
    volatile String inputTopicName;

    @ConfigData(key = "map.validation.broadcastRateTopicName", 
        description = "Output topic for Broadcast Rate events", 
        updateType = READ_ONLY)
    volatile String broadcastRateTopicName;

    @ConfigData(key = "map.validation.minimumDataTopicName",
        description = "Output topic for Minimum Data events",
        updateType = READ_ONLY)
    volatile String minimumDataTopicName;

    @ConfigData(key = "map.validation.broadcast-rate-notification-topic-name",
        description = "Output topic for Broadcast Rate notifications",
        updateType = READ_ONLY)
    volatile String broadcastRateNotificationTopicName;

    @ConfigData(key = "map.validation.v2-broadcast-rate-buffer-size-seconds",
        units = SECONDS,
        description = "Broadcast rate buffer size for V2 Broadcast Rate algorithm",
        updateType = READ_ONLY)
    volatile int v2BroadcastRateBufferSizeSeconds;

    @ConfigData(key = "map.validation.v2-broadcast-rate-buffer-grace-period-ms",
        units = MILLISECONDS,
        description = "Broadcast rate buffer grace period for V2 Broadcast Rate algorithm",
        updateType = READ_ONLY)
    volatile int v2BroadcastRateBufferGracePeriodMs;

    @ConfigData(key = "map.validation.v2-broadcast-rate-lower-bound-pair-separation-ms",
        units = MILLISECONDS,
        description = "Broadcast rate lower bound pair separation for V2 Broadcast Rate algorithm",
        updateType = READ_ONLY)
    volatile int v2BroadcastRateLowerBoundPairSeparationMs;

    @ConfigData(key = "map.validation.v2-broadcast-rate-upper-bound-pair-separation-ms",
        units = MILLISECONDS,
        description = "Broadcast rate upper bound pair separation for V2 Broadcast Rate algorithm",
        updateType = READ_ONLY)
    volatile int v2BroadcastRateUpperBoundPairSeparationMs;

    @ConfigData(key = "map.validation.v2-broadcast-rate-lower-bound-duration-per-10-messages-ms",
        units = MILLISECONDS,
        description = "Broadcast rate lower bound duration per 10 messages for V2 Broadcast Rate algorithm",
        updateType = READ_ONLY)
    volatile int v2BroadcastRateLowerBoundDurationPer10MessagesMs;

    @ConfigData(key = "map.validation.v2-broadcast-rate-upper-bound-duration-per-10-messages-ms",
        units = MILLISECONDS,
        description = "Broadcast rate upper bound duration per 10 messages for V2 Broadcast Rate algorithm",
        updateType = READ_ONLY)
    volatile int v2BroadcastRateUpperBoundDurationPer10MessagesMs;

    @ConfigData(key = "map.validation.v2-broadcast-rate-conformance-percent",
        description = "Broadcast rate conformance percent for V2 Broadcast Rate algorithm",
        updateType = READ_ONLY)
    volatile int v2BroadcastRateConformancePercent;

    @ConfigData(key = "map.validation.v2-broadcast-rate-assessment-window-duration",
        description = "Broadcast rate assessment window duration for V2 Broadcast Rate algorithm",
        updateType = READ_ONLY)
    volatile int v2BroadcastRateAssessmentWindowDuration;

    @ConfigData(key = "map.validation.v2-broadcast-rate-assessment-window-duration-units",
        description = "Broadcast rate assessment window duration units",
        updateType = READ_ONLY)
    volatile ChronoUnit v2BroadcastRateAssessmentWindowDurationUnits;

    @ConfigData(key = "map.validation.v2-broadcast-rate-assessment-window-grace-period-ms",
        units = MILLISECONDS,
        description = "Broadcast rate assessment window grace period for V2 Broadcast Rate algorithm",
        updateType = READ_ONLY)
    volatile int v2BroadcastRateAssessmentWindowGracePeriodMs;


    @ConfigData(key ="map.validation.rollingPeriodSeconds", 
        units = SECONDS, 
        description = "The aggregation window size", 
        updateType = DEFAULT)
    volatile int rollingPeriodSeconds;

    @ConfigData(key = "map.validation.outputIntervalSeconds", 
        units = SECONDS, 
        description = "The window hop", 
        updateType = DEFAULT)
    volatile int outputIntervalSeconds;

    @ConfigData(key = "map.validation.gracePeriodMilliseconds", 
        units = MILLISECONDS, 
        description = "Window grace period", 
        updateType = DEFAULT)
    volatile int gracePeriodMilliseconds;
    
    @ConfigData(key = "map.validation.lowerBound", 
        units = PER_PERIOD, 
        description = "Exclusive minimum counts per period", 
        updateType = INTERSECTION)
    volatile int lowerBound;

    @ConfigData(key = "map.validation.upperBound", 
        units = PER_PERIOD, 
        description = "Exclusive maximum counts per period", 
        updateType = INTERSECTION)
    volatile int upperBound;
    
    @ConfigData(key = "map.validation.debug", 
        description = "Whether to log diagnostic info", 
        updateType = INTERSECTION)
    volatile boolean debug;

    @ConfigData(key = "map.validation.aggregateEvents",
            description = "Whether to aggregate output minimum data events, or to send each individual event",
            updateType = READ_ONLY)
    boolean aggregateMinimumDataEvents;

    //
    // Maps for parameters that can be customized at the intersection level
    //
    final ConfigMap<Integer> lowerBoundMap = new ConfigMap<>();
    final ConfigMap<Integer> upperBoundMap = new ConfigMap<>();
    final ConfigMap<Boolean> debugMap = new ConfigMap<>();

    //
    // Intersection-specific properties 
    //
    public int getLowerBound(IntersectionRegion intersectionKey) {
        return getIntersectionValue(intersectionKey, lowerBoundMap, lowerBound);
    }
    public int getUpperBound(IntersectionRegion intersectionKey) {
        return getIntersectionValue(intersectionKey, upperBoundMap, upperBound);
    }
    public boolean getDebug(IntersectionRegion intersectionKey) {
        return getIntersectionValue(intersectionKey, debugMap, debug);
    }
}
