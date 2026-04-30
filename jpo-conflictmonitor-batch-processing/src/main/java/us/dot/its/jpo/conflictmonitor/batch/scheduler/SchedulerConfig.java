package us.dot.its.jpo.conflictmonitor.batch.scheduler;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

@Configuration
public class SchedulerConfig implements SchedulingConfigurer {

    private final int threadPoolSize;

    public SchedulerConfig(@Value("${cm.atspm.scheduler.pool.size}") int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.setScheduler(taskScheduler());
    }

    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        var scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(threadPoolSize);
        scheduler.setThreadNamePrefix("ScheduledTasks-");
        scheduler.setVirtualThreads(true);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduler.initialize();
        return scheduler;
    }
}
