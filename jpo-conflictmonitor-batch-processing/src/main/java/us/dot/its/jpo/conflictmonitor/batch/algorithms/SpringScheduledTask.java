package us.dot.its.jpo.conflictmonitor.batch.algorithms;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.time.Clock;

/**
 * Base class for scheduled tasks based on Spring Scheduling
 */
@Slf4j
@ToString
@Data
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public abstract class SpringScheduledTask<TaskMetadata, TParameters> implements Runnable {

    protected TaskMetadata taskMetadata;
    protected TParameters parameters;
    protected Clock clock;

}
