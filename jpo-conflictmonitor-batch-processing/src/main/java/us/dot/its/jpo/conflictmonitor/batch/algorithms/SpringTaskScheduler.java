package us.dot.its.jpo.conflictmonitor.batch.algorithms;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

@Slf4j
@ToString
public abstract class SpringTaskScheduler<TParameters, TaskMetadata, TTask extends Runnable>
    implements Algorithm<TParameters>, ScheduledTaskAlgorithm<TaskMetadata> {

    protected Duration interval;
    protected Instant startTime;
    protected TParameters parameters;
    protected List<TaskMetadata> taskMetadata;
    protected final ThreadPoolTaskScheduler taskScheduler;
    protected final List<ScheduledFuture<?>> futureTasks = new ArrayList<>();

    public SpringTaskScheduler(ThreadPoolTaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    @Override
    public void setInterval(Duration interval) {
        this.interval = interval;
    }

    @Override
    public Duration getInterval() {
        return interval;
    }

    @Override
    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    @Override
    public Instant getStartTime() {
        return startTime;
    }

    @Override
    public void start() {
        validate();
        for (TaskMetadata taskMetadata : taskMetadata) {
            var task = createTask(taskMetadata, parameters);
            var futureTask = taskScheduler.scheduleAtFixedRate(task, startTime, interval);
            futureTasks.add(futureTask);
            log.info("Task {} scheduled at {} with interval {}", this, startTime, interval);
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

    @Override
    public void setParameters(TParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public TParameters getParameters() {
        return parameters;
    }

    /**
     * Check that required parameters are set
     */
    private void validate() {
        if (interval == null) {
            throw new IllegalStateException("interval is not initialized");
        }
        if (startTime == null) {
            throw new IllegalStateException("startTime is not initialized");
        }
        if (parameters == null) {
            throw new IllegalStateException("parameters are not initialized");
        }
        if (taskMetadata == null || taskMetadata.isEmpty()) {
            throw new IllegalStateException("task metadata is not initialized or is empty list");
        }
    }

}
