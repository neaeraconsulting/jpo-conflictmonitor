package us.dot.its.jpo.conflictmonitor.monitor.utils;

import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Set;

import static java.time.temporal.ChronoUnit.*;
import static java.time.temporal.ChronoUnit.HOURS;

/**
 * Utilities to calculate aligned time periods for aggregation and metrics.
 */
@Slf4j
public class TimePeriodCalculator {


    /**
     * @return Aggregated event state store retention time in milliseconds, calculated to include 2 aggregation
     * intervals and grace periods.
     */
    public static long retentionTimeMs(int interval, ChronoUnit intervalUnits, long gracePeriodMs) {
        return 2 * (aggIntervalMs(interval, intervalUnits) + gracePeriodMs);
    }

    private static Duration aggInterval(int interval, ChronoUnit intervalUnits) {
        return Duration.of(interval, intervalUnits);
    }

    /**
     * @return Aggregation interval in epoch milliseconds
     */
    private static long aggIntervalMs(int interval, ChronoUnit intervalUnits) {
        return aggInterval(interval, intervalUnits).toMillis();
    }



    /**
     * Utility function to get the beginning and end of the aggregation interval aligned to the beginning of the day,
     * hour, or minute, containing the timestamp.
     *
     * @param timestampMs Epoch millisecond timestamp
     * @return A midnight aligned time period (begin and end time) containing the timestamp.
     */
    public static ProcessingTimePeriod aggTimePeriod(final long timestampMs, final int interval, final ChronoUnit intervalUnits) {
        validateInterval(interval, intervalUnits);
        final var timestamp = Instant.ofEpochMilli(timestampMs);
        final var zdt = ZonedDateTime.ofInstant(timestamp, ZoneOffset.UTC);
        return switch (intervalUnits) {
            case HOURS -> {
                final Instant startOfDay = ZonedDateTime.of(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(),
                        0, 0, 0, 0, ZoneOffset.UTC).toInstant();
                final int numHours = zdt.getHour();
                yield alignedTimePeriod(startOfDay, numHours, interval, intervalUnits);
            }
            case MINUTES -> {
                final Instant startOfHour = ZonedDateTime.of(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(),
                        zdt.getHour(), 0, 0, 0, ZoneOffset.UTC).toInstant();
                final int numMinutes = zdt.getMinute();
                yield alignedTimePeriod(startOfHour, numMinutes, interval, intervalUnits);
            }
            case SECONDS -> {
                final Instant startOfMinute = ZonedDateTime.of(zdt.getYear(), zdt.getMonthValue(), zdt.getDayOfMonth(),
                        zdt.getHour(), zdt.getMinute(), 0, 0, ZoneOffset.UTC).toInstant();
                final int numSeconds = zdt.getSecond();
                yield alignedTimePeriod(startOfMinute, numSeconds, interval, intervalUnits);
            }
            default -> throw new RuntimeException("Invalid interval units");
        };

    }

    private static ProcessingTimePeriod alignedTimePeriod(final Instant startOfUnits, final int numUnits,
                                                          int interval, ChronoUnit intervalUnits) {
        final var numberOfIntervals = numUnits / interval;
        final var startOfInterval = startOfUnits.plus((long) numberOfIntervals * interval, intervalUnits);
        final var endOfInterval = startOfInterval.plus(interval, intervalUnits);
        final var period = new ProcessingTimePeriod();
        period.setBeginTimestamp(startOfInterval.toEpochMilli());
        period.setEndTimestamp(endOfInterval.toEpochMilli());
        return period;
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
    public static boolean validateInterval(int interval, ChronoUnit intervalUnits) {
        if (!allowedIntervalUnits.contains(intervalUnits)) {
            log.error("Invalid interval units: {}, allowed values are: {}", intervalUnits, allowedIntervalUnits);
            return false;
        }

        if (intervalUnits.equals(HOURS)) {
            if (interval < 0 || 24 % interval != 0) {
                log.error("Invalid time interval: {}, allowed values are: 1, 2, 3, 4, 6, 8, 24 hours", interval);
                return false;
            } else {
                return true;
            }
        }

        // SECONDS or MINUTES
        if (interval < 0 || 60 % interval != 0) {
            log.error("Invalid time interval: {}, allowed values are: 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60 {}", interval, intervalUnits);
            return false;
        } else {
            return true;
        }
    }


    private static final Set<ChronoUnit> allowedIntervalUnits
            = ImmutableSet.of(SECONDS, MINUTES, HOURS);
}
