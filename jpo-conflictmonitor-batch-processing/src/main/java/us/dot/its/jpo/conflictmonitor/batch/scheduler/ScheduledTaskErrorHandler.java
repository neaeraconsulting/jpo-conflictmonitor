package us.dot.its.jpo.conflictmonitor.batch.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.util.ErrorHandler;

/**
 * Error handler for scheduled tasks.
 */
@Slf4j
@Component
public class ScheduledTaskErrorHandler implements ErrorHandler {
    @Override
    public void handleError(Throwable e) {
        log.error("Exception occurred while executing scheduled task", e);
    }
}
