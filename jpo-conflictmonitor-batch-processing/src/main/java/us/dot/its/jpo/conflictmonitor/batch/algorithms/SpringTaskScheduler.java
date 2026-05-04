package us.dot.its.jpo.conflictmonitor.batch.algorithms;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

@Getter
@Setter
@Slf4j
@ToString
public abstract class SpringTaskScheduler<TParameters, TaskMetadata, TTask extends Runnable>
    implements ScheduledTaskAlgorithm<TParameters, TaskMetadata> {

    protected int interval;
    protected ChronoUnit intervalUnits;
    protected int taskStartTimeStagger;
    protected ChronoUnit taskStartTimeStaggerUnits;
    protected int gracePeriodOffset;
    protected ChronoUnit gracePeriodOffsetUnits;
    protected TParameters parameters;
    protected List<TaskMetadata> taskMetadata;
    protected final ThreadPoolTaskScheduler taskScheduler;
    protected final List<ScheduledFuture<?>> futureTasks = new ArrayList<>();
    protected final Clock clock;

    public SpringTaskScheduler(ThreadPoolTaskScheduler taskScheduler, Clock clock) {
        this.taskScheduler = taskScheduler;
        this.clock = clock;
    }


    @Override
    public void start() {
        validate();
        // Start at top of the specified unit (e.g. top of the hour if interval units are hours.)
        final Instant now = clock.instant();

        final Instant startTime = switch (intervalUnits) {
            case ChronoUnit.HOURS -> now.truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS);
            case ChronoUnit.MINUTES -> now.truncatedTo(ChronoUnit.MINUTES).plus(1, ChronoUnit.MINUTES);
            case ChronoUnit.SECONDS -> now.truncatedTo(ChronoUnit.SECONDS).plus(1, ChronoUnit.SECONDS);
            default -> now;
        };
        Instant offsetStartTime = startTime;
        for (TaskMetadata taskMetadata : taskMetadata) {
            log.info("Start time {}", offsetStartTime);
            var task = createTask(taskMetadata, parameters);
            var futureTask = taskScheduler.scheduleAtFixedRate(task, offsetStartTime, Duration.of(interval, intervalUnits));
            futureTasks.add(futureTask);
            log.info("Task for {} scheduled at {} with interval {} {}", taskMetadata, offsetStartTime, interval, intervalUnits);
            offsetStartTime = offsetStartTime.plus(taskStartTimeStagger, taskStartTimeStaggerUnits);
        }
    }

    protected abstract TTask createTask(TaskMetadata taskMetadata, TParameters parameters);

    @Override
    public void stop() {
        for (ScheduledFuture<?> futureTask : futureTasks) {
            if (futureTask != null) {
                futureTask.cancel(true);
            }
        }
        futureTasks.clear();
    }



    /**
     * Check that required parameters are set
     */
    private void validate() {
        if (interval <= 0) {
            throw new IllegalStateException("interval is not initialized");
        }
        if (intervalUnits == null) {
            throw new IllegalStateException("intervalUnits is not initialized");
        }
        if (taskStartTimeStagger <= 0) {
            throw new IllegalStateException("taskStartTimeStagger is not initialized");
        }
        if (taskStartTimeStaggerUnits == null) {
            throw new IllegalStateException("taskStartTimeStaggerUnits is not initialized");
        }
        if (gracePeriodOffset <= 0) {
            throw new IllegalStateException("gracePeriodOffset is not initialized");
        }
        if (gracePeriodOffsetUnits == null) {
            throw new IllegalStateException("gracePeriodOffsetUnits is not initialized");
        }
        if (parameters == null) {
            throw new IllegalStateException("parameters are not initialized");
        }
        if (taskMetadata == null || taskMetadata.isEmpty()) {
            throw new IllegalStateException("task metadata is not initialized or is empty list");
        }
    }


}
