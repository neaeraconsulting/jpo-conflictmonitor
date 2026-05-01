package us.dot.its.jpo.conflictmonitor.batch.algorithms;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Base class for scheduled tasks based on Spring Scheduling
 */
@Slf4j
@ToString
@Data
public abstract class SpringScheduledTask<TaskMetadata, TParameters> implements Runnable {

    protected TParameters parameters;
    protected TaskMetadata taskMetadata;

    public SpringScheduledTask(TaskMetadata taskMetadata, TParameters parameters) {
        this.taskMetadata = taskMetadata;
        this.parameters = parameters;
    }

}
