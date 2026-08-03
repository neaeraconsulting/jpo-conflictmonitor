package us.dot.its.jpo.conflictmonitor.batch.time;

import java.time.*;

public class TimeUtil {

    /**
     * Convert a local time to an instant with a given time zone id.
     * Since ZoneOffset does not have a 1-1 relationship with ZoneId for all zones, a two step
     * iterative process is used to determine the best guess for the ZoneOffset.
     * For time zones with DST changes, the offset may be off by one within 1 hour of "spring forward" or
     * "fall back" time, but it will always be correct for America/Phoenix time which doesn't have DST.
     *
     * @param localTime The local time to be converted
     * @param zoneId The time zone id
     * @param clock The clock
     * @return An instant at the time zone
     */
    public static Instant localTimeToInstantAtZone(LocalDateTime localTime, ZoneId zoneId, Clock clock) {
        Instant now = clock.instant();
        ZoneOffset zoneOffset0 = zoneId.getRules().getOffset(now);
        Instant instant0 = localTime.atZone(zoneOffset0).toInstant();
        ZoneOffset zoneOffset1 = zoneId.getRules().getOffset(instant0);
        return localTime.atZone(zoneOffset1).toInstant();
    }
}
