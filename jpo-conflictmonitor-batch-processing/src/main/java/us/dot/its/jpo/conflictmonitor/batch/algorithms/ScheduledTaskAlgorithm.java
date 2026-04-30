package us.dot.its.jpo.conflictmonitor.batch.algorithms;

import java.time.Duration;
import java.time.Instant;

/***
 * Interface for an algorithm implemented as a scheduled task that performs batch
 * processing at a scheduled interval, with configurable parameters, that can
 * be started and stopped.
 */
public interface ScheduledTaskAlgorithm extends Runnable {

    // Scheduling params
    void setInterval(Duration interval);
    Duration getInterval();
    void setStartTime(Instant startTime);
    Instant getStartTime();

}
