package us.dot.its.jpo.conflictmonitor.batch.scheduler;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.util.ErrorHandler;

@Configuration
public class SchedulerConfig implements SchedulingConfigurer {

    private final int threadPoolSize;
    private final ErrorHandler errorHandler;

    public SchedulerConfig(@Value("${cm.batch.scheduler.thread.pool.size}") int threadPoolSize, @Autowired ErrorHandler errorHandler) {
        this.threadPoolSize = threadPoolSize;
        this.errorHandler = errorHandler;
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
        scheduler.setErrorHandler(errorHandler);
        return scheduler;
    }

}
