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
    implements Algorithm<TParameters>, ScheduledTaskAlgorithm<TaskMetadata, TParameters> {

    protected Integer interval;
    protected ChronoUnit intervalUnits;
    protected Integer taskStartTimeStagger;
    protected ChronoUnit taskStartTimeStaggerUnits;
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
        Instant now = clock.instant();
        Instant startTime = switch (intervalUnits) {
            case ChronoUnit.HOURS -> now.truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS);
            case ChronoUnit.MINUTES -> now.truncatedTo(ChronoUnit.MINUTES).plus(1, ChronoUnit.MINUTES);
            case ChronoUnit.SECONDS -> now.truncatedTo(ChronoUnit.SECONDS).plus(1, ChronoUnit.SECONDS);
            default -> now;
        };
        log.info("Start time {}", startTime);
        for (TaskMetadata taskMetadata : taskMetadata) {
            var task = createTask(taskMetadata, parameters);
            var futureTask = taskScheduler.scheduleAtFixedRate(task, startTime, Duration.of(interval, intervalUnits));
            futureTasks.add(futureTask);
            log.info("Task {} scheduled at {} with interval {} {}", this, startTime, interval, intervalUnits);
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
        if (interval == null) {
            throw new IllegalStateException("interval is not initialized");
        }
        if (parameters == null) {
            throw new IllegalStateException("parameters are not initialized");
        }
        if (taskMetadata == null || taskMetadata.isEmpty()) {
            throw new IllegalStateException("task metadata is not initialized or is empty list");
        }
    }

}
