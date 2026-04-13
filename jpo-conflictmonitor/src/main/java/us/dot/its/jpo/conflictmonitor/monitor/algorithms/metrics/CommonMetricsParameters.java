package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics;

import lombok.Data;
import lombok.Generated;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigData;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigDataClass;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.UnitsEnum;

import java.time.temporal.ChronoUnit;

import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UpdateType.READ_ONLY;

/**
 * Common parameters for all metrics to specify the interval to aggregate
 * metrics over.
 */
@Data
@Generated
@Component
@ConfigurationProperties(prefix = "metrics.common")
@ConfigDataClass
@Slf4j
public class CommonMetricsParameters {

    @ConfigData(key = "metrics.common.interval",
            description = "The time interval over which to aggregate metrics.",
            updateType = READ_ONLY)
    private int interval;

    @ConfigData(key = "metrics.common.intervalUnits",
            description = "The time units of the metrics aggregation interval",
            updateType = READ_ONLY)
    private ChronoUnit intervalUnits;

    @ConfigData(key = "metrics.common.gracePeriodMs",
            description = "Grace period for receiving out-of-order events to be used in calculating the metric",
            updateType = READ_ONLY,
            units = UnitsEnum.MILLISECONDS)
    private long gracePeriodMs;

    @ConfigData(key = "metrics.common.checkInterval",
            description = "The interval to check for events that have stopped being sent.  Should be on the same" +
                    "order or smaller than the aggregation interval.",
            updateType = READ_ONLY)
    private int checkInterval;

    @ConfigData(key = "metrics.common.checkIntervalUnits",
            description = "The time units of the check interval",
            updateType = READ_ONLY)
    private ChronoUnit checkIntervalUnits;

    @ConfigData(key = "metrics.common.retentionTime",
            description = "The time to retain timestamps for events that have stopped being during an aggregation " +
                    "window. Should be longer than the aggregation interval.",
            updateType = READ_ONLY)
    private int retentionTime;

    @ConfigData(key = "metrics.common.retentionTimeUnits",
            description = "Time units for the retention time",
            updateType = READ_ONLY)
    private ChronoUnit retentionTimeUnits;

}
