package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics;

import lombok.Data;
import lombok.Generated;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigData;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigDataClass;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;
import us.dot.its.jpo.conflictmonitor.monitor.utils.TimePeriodCalculator;

import java.time.temporal.ChronoUnit;

import static us.dot.its.jpo.conflictmonitor.monitor.models.config.UpdateType.DEFAULT;
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

    @ConfigData(key = "metrics.common.debug",
            description = "Whether to log diagnostic information for debugging",
            updateType = DEFAULT)
    private volatile boolean debug;

    @ConfigData(key = "metrics.common.interval",
            description = "The time interval over which to aggregate metrics",
            updateType = READ_ONLY)
    private int interval;

    @ConfigData(key = "metrics.common.interval.units",
            description = "The time units of the metrics aggregation interval",
            updateType = READ_ONLY)
    private ChronoUnit intervalUnits;

    @ConfigData(key = "metrics.common.punctuator.interval.ms",
            description = "How often to run the process to check whether to emit metrics",
            updateType = READ_ONLY)
    private long checkIntervalMs;

    @ConfigData(key = "metrics.common.grace.period.ms",
            description = "Grace period for receiving out-of-order events to be used in calculating the metric",
            updateType = READ_ONLY)
    private long gracePeriodMs;

    /**
     * @return Aggregated event state store retention time in milliseconds, calculated to include 2 aggregation
     * intervals and grace periods.
     */
    public long retentionTimeMs() {
        return TimePeriodCalculator.retentionTimeMs(interval, intervalUnits, gracePeriodMs);
    }

    /**
     * Utility function to get the beginning and end of the aggregation interval aligned to the beginning of the day,
     * hour, or minute, containing the timestamp.
     *
     * @param timestampMs Epoch millisecond timestamp
     * @return A midnight aligned time period (begin and end time) containing the timestamp.
     */
    public ProcessingTimePeriod aggTimePeriod(final long timestampMs) {
        return TimePeriodCalculator.aggTimePeriod(timestampMs, interval, intervalUnits);

    }


    /**
     * Preliminary Design Details, Data Management, p.9:
     * <pre>
     * The user shall only be able to select intervals that are
     * • a factor of 60 seconds (if less than 60 seconds)
     *     e.g. 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60 seconds
     * • OR a factor of 60 minutes (between 1 minute and 60 minutes)
     *     e.g. 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60 minutes
     * • OR a factor of 24 hours (between 1 hour and 24 hours)
     *     e.g. 1, 2, 3, 4, 6, 8, 24 hours
     * </pre>
     * @return valid or not
     */
    public boolean validateInterval() {
        return TimePeriodCalculator.validateInterval(interval, intervalUnits);
    }

}
