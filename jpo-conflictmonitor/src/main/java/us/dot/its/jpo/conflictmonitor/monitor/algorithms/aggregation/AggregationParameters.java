package us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation;

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

@Data
@Generated
@Component
@ConfigurationProperties(prefix = "aggregation")
@ConfigDataClass
@Slf4j
public class AggregationParameters {

    @ConfigData(key = "aggregation.debug",
            description = "Whether to log diagnostic information for debugging",
            updateType = DEFAULT)
    volatile boolean debug;

    @ConfigData(key = "aggregation.interval",
            description = "The time interval over which to aggregate events",
            updateType = READ_ONLY)
    volatile int interval;

    @ConfigData(key = "aggregation.interval.units",
            description = "The time units of the aggregation interval",
            updateType = READ_ONLY)
    volatile ChronoUnit intervalUnits;

    @ConfigData(key = "aggregation.punctuator.interval.ms",
            description = """
            How often to run the process to check whether to emit an aggregated event, in milliseconds. Must be shorter
            than the aggregation interval. Publishing the aggregated events may be delayed by up to this amount of time
            after the aggregation period elapses.
            """,
            updateType = READ_ONLY)
    long checkIntervalMs;

    @ConfigData(key = "aggregation.grace.period.ms",
            description = "Grace period for receiving out-of-order events",
            updateType = READ_ONLY)
    volatile long gracePeriodMs;

    @ConfigData(key = "aggregation.eventTopicMap",
        description = "Map of aggregated event names to output topic names",
        updateType = READ_ONLY)
    EventTopicMap eventTopicMap;

    @ConfigData(key = "aggregation.eventAlgorithmMap", description = "Map of aggregated event names to algorithm names",
        updateType = READ_ONLY)
    EventAlgorithmMap eventAlgorithmMap;



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
