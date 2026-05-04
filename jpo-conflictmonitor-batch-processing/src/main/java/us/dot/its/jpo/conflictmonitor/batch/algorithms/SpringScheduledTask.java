package us.dot.its.jpo.conflictmonitor.batch.algorithms;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;
import java.time.Duration;

/**
 * Base class for scheduled tasks based on Spring Scheduling
 */
@Slf4j
@ToString
@Data
public abstract class SpringScheduledTask<TaskMetadata, TParameters> implements Runnable {

    protected TParameters parameters;
    protected TaskMetadata taskMetadata;
    protected Clock clock;

    public SpringScheduledTask(TaskMetadata taskMetadata, TParameters parameters, Clock clock) {
        this.taskMetadata = taskMetadata;
        this.parameters = parameters;
        this.clock = clock;
    }

}
