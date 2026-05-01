package us.dot.its.jpo.conflictmonitor.batch.algorithms;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ScheduledFuture;

/**
 * Base class for scheduled tasks based on Spring Scheduling
 */
@Slf4j
@ToString
public abstract class SpringScheduledTask<TParameters>
        implements Algorithm<TParameters>, ScheduledTaskAlgorithm {

    protected Duration interval;
    protected Instant startTime;
    protected TParameters parameters;
    protected final ThreadPoolTaskScheduler taskScheduler;
    protected ScheduledFuture<?> futureTask;

    public SpringScheduledTask(ThreadPoolTaskScheduler taskScheduler) {
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
        futureTask = taskScheduler.scheduleAtFixedRate(this, startTime, interval);
        log.info("Task {} scheduled at {} with interval {}", this, startTime, interval);
    }

    @Override
    public void stop() {
        if (futureTask != null) {
            futureTask.cancel(true);
            futureTask = null;
        }
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
    }

}
