package us.dot.its.jpo.conflictmonitor.batch.algorithms;

import java.time.temporal.ChronoUnit;
import java.util.List;

/***
 * Interface for an algorithm implemented as a scheduled task that performs batch
 * processing at a scheduled interval, with configurable parameters, that can
 * be started and stopped.
 */
public interface ScheduledTaskAlgorithm<TParameters, TaskMetadata> extends Algorithm<TParameters> {

    // Scheduling params
    void setInterval(int interval);
    int getInterval();
    void setIntervalUnits(ChronoUnit intervalUnits);
    ChronoUnit getIntervalUnits();
    void setTaskStartTimeStagger(int taskStartTimeStagger);
    int getTaskStartTimeStagger();
    void setTaskStartTimeStaggerUnits(ChronoUnit taskStartTimeStaggerUnits);
    ChronoUnit getTaskStartTimeStaggerUnits();

    // Set separate metadata for each scheduled task
    // Each scheduled task runs in parallel, for scalability.
    // Each metadata list item represents one scheduled task
    // Choosing metadata to divide up the tasks is equivalent to choosing the partitioning in a streams or database
    // application.
    void setTaskMetadata(List<TaskMetadata> taskMetadataList);
    List<TaskMetadata> getTaskMetadata();
}
