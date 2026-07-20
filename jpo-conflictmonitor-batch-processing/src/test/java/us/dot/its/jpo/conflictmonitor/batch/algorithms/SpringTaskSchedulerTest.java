package us.dot.its.jpo.conflictmonitor.batch.algorithms;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.ErrorHandler;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Covers SpringTaskScheduler: the parameter validation in start(), the per-task-metadata
 * scheduling/staggering logic, and stop(). Uses a minimal local subclass (SpringTaskScheduler
 * is abstract) rather than pulling in a real algorithm's dependency graph, to isolate this
 * base class's own logic.
 */
@ExtendWith(MockitoExtension.class)
class SpringTaskSchedulerTest {

    private static final Instant NOW = Instant.parse("2026-05-03T10:15:30Z");

    @Mock
    private ThreadPoolTaskScheduler taskScheduler;
    @Mock
    private ErrorHandler errorHandler;

    private static class TestTaskScheduler extends SpringTaskScheduler<String, String, Runnable> {
        TestTaskScheduler(ThreadPoolTaskScheduler taskScheduler, Clock clock, ErrorHandler errorHandler) {
            super(taskScheduler, clock, errorHandler);
        }

        @Override
        protected Runnable createTask(String taskMetadata, String parameters) {
            return () -> {};
        }
    }

    private TestTaskScheduler validScheduler() {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        var scheduler = new TestTaskScheduler(taskScheduler, clock, errorHandler);
        scheduler.setInterval(1);
        scheduler.setIntervalUnits(ChronoUnit.HOURS);
        scheduler.setTaskStartTimeStagger(2);
        scheduler.setTaskStartTimeStaggerUnits(ChronoUnit.MINUTES);
        scheduler.setGracePeriodOffset(1);
        scheduler.setGracePeriodOffsetUnits(ChronoUnit.HOURS);
        scheduler.setParameters("params");
        scheduler.setTaskMetadata(List.of("route1"));
        return scheduler;
    }

    private void stubScheduling() {
        when(taskScheduler.scheduleAtFixedRate(any(Runnable.class), any(Instant.class), any(Duration.class)))
                .thenAnswer(invocation -> mock(ScheduledFuture.class));
    }

    @Test
    void startThrowsWhenIntervalIsNotPositive() {
        var scheduler = validScheduler();
        scheduler.setInterval(0);

        assertThrows(IllegalStateException.class, scheduler::start);
    }

    @Test
    void startThrowsWhenIntervalUnitsIsNull() {
        var scheduler = validScheduler();
        scheduler.setIntervalUnits(null);

        assertThrows(IllegalStateException.class, scheduler::start);
    }

    @Test
    void startThrowsWhenTaskStartTimeStaggerIsNotPositive() {
        var scheduler = validScheduler();
        scheduler.setTaskStartTimeStagger(0);

        assertThrows(IllegalStateException.class, scheduler::start);
    }

    @Test
    void startThrowsWhenGracePeriodOffsetIsNotPositive() {
        var scheduler = validScheduler();
        scheduler.setGracePeriodOffset(0);

        assertThrows(IllegalStateException.class, scheduler::start);
    }

    @Test
    void startThrowsWhenParametersAreNull() {
        var scheduler = validScheduler();
        scheduler.setParameters(null);

        assertThrows(IllegalStateException.class, scheduler::start);
    }

    @Test
    void startThrowsWhenTaskMetadataIsNullOrEmpty() {
        var scheduler = validScheduler();
        scheduler.setTaskMetadata(List.of());

        assertThrows(IllegalStateException.class, scheduler::start);
    }

    @Test
    void startSchedulesOneTaskPerTaskMetadataEntry() {
        var scheduler = validScheduler();
        scheduler.setTaskMetadata(List.of("route1", "route2", "route3"));
        stubScheduling();

        scheduler.start();

        verify(taskScheduler, times(3)).scheduleAtFixedRate(any(Runnable.class), any(Instant.class), any(Duration.class));
    }

    @Test
    void startStaggersEachSubsequentTasksStartTimeByTheConfiguredOffset() {
        var scheduler = validScheduler();
        scheduler.setTaskMetadata(List.of("route1", "route2"));
        scheduler.setTaskStartTimeStagger(5);
        scheduler.setTaskStartTimeStaggerUnits(ChronoUnit.MINUTES);
        stubScheduling();

        scheduler.start();

        ArgumentCaptor<Instant> startTimeCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(taskScheduler, times(2)).scheduleAtFixedRate(any(Runnable.class), startTimeCaptor.capture(), any(Duration.class));
        List<Instant> startTimes = startTimeCaptor.getAllValues();
        assertThat(startTimes.get(1), is(startTimes.getFirst().plus(5, ChronoUnit.MINUTES)));
    }

    @Test
    void startTruncatesTheFirstStartTimeToTheTopOfTheNextIntervalUnit() {
        // NOW = 2026-05-03T10:15:30Z, intervalUnits = HOURS -> top of the next hour
        var scheduler = validScheduler();
        stubScheduling();

        scheduler.start();

        ArgumentCaptor<Instant> startTimeCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(taskScheduler).scheduleAtFixedRate(any(Runnable.class), startTimeCaptor.capture(), any(Duration.class));
        assertThat(startTimeCaptor.getValue(), is(Instant.parse("2026-05-03T11:00:00Z")));
    }

    @Test
    void stopCancelsAllScheduledFuturesAndClearsThem() {
        var scheduler = validScheduler();
        scheduler.setTaskMetadata(List.of("route1", "route2"));
        var future1 = mock(ScheduledFuture.class);
        var future2 = mock(ScheduledFuture.class);
        when(taskScheduler.scheduleAtFixedRate(any(Runnable.class), any(Instant.class), any(Duration.class)))
                .thenReturn(future1, future2);
        scheduler.start();

        scheduler.stop();

        verify(future1).cancel(true);
        verify(future2).cancel(true);
        assertThat(scheduler.getFutureTasks(), is(empty()));
    }
}
