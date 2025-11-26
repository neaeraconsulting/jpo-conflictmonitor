package us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.rtcm;

import lombok.Data;
import lombok.Generated;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.models.IntersectionRegion;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigData;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigDataClass;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigMap;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.config.ConfigUtil.getIntersectionValue;
import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UnitsEnum.*;
import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UnitsEnum.PER_PERIOD;
import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UpdateType.*;

@Data
@Generated
@Component
@ConfigurationProperties(prefix = "rtcm.validation")
@ConfigDataClass
public class RtcmValidationParameters {

    @ConfigData(key = "rtcm.validation.inputTopicName",
            description = "Input kafka topic",
            updateType = READ_ONLY)
    String inputTopicName;

    @ConfigData(key = "rtcm.validation.broadcastRateTopicName",
            description = "Output topic for Broadcast Rate events",
            updateType = READ_ONLY)
    String broadcastRateTopicName;

    @ConfigData(key = "rtcm.validation.minimumDataTopicName",
            description = "Output topic for Minimum Data events",
            updateType = READ_ONLY)
    String minimumDataTopicName;


    @ConfigData(key ="rtcm.validation.rollingPeriodSeconds",
            units = SECONDS,
            description = "The aggregation window size",
            updateType = DEFAULT)
    volatile int rollingPeriodSeconds;

    @ConfigData(key = "rtcm.validation.outputIntervalSeconds",
            units = SECONDS,
            description = "The window hop",
            updateType = DEFAULT)
    volatile int outputIntervalSeconds;

    @ConfigData(key = "rtcm.validation.gracePeriodMilliseconds",
            units = MILLISECONDS,
            description = "Window grace period",
            updateType = DEFAULT)
    volatile int gracePeriodMilliseconds;

    @ConfigData(key = "rtcm.validation.lowerBound",
            units = PER_PERIOD,
            description = "Exclusive minimum counts per period for basic message types. Configure with some slack as" +
                    " needed, nominally should be equivalent to 1 Hz per CTI-4501 4.3.3.5.2.2",
            updateType = INTERSECTION)
    volatile int lowerBound;

    @ConfigData(key = "rtcm.validation.upperBound",
            units = PER_PERIOD,
            description = "Exclusive maximum counts per period for basic message types. Configure with some slack as" +
                    " needed, nominally should be equivalent to 1 Hz per CTI-4501 4.3.3.5.2.2",
            updateType = INTERSECTION)
    volatile int upperBound;

    @ConfigData(key = "rtcm.validation.msmLowerBound",
        units = PER_PERIOD,
        description = "Exclusive minimum counts per period for MSM 4 message types.  Configure with some slack as" +
                " needed, nominally should be equivalent to 1 Hz per CTI-4501 4.3.3.5.2.2",
        updateType = INTERSECTION)
    volatile int msmLowerBound;

    @ConfigData(key = "rtcm.validation.msmUpperBound",
        units = PER_PERIOD,
        description = "Exclusive maximum counts per period for MSM message types. Configure with some slack as" +
                " needed, nominally should be equivalent to 10 Hz per CTI-4501 4.3.3.5.2.2",
        updateType = INTERSECTION)
    volatile int msmUpperBound;

    @ConfigData(key = "rtcm.validation.debug",
            description = "Whether to log diagnostic info",
            updateType = INTERSECTION)
    volatile boolean debug;

    @ConfigData(key = "rtcm.validation.aggregateEvents",
            description = "Whether to aggregate output minimum data events, or to send each individual event",
            updateType = READ_ONLY)
    boolean aggregateMinimumDataEvents;


}
