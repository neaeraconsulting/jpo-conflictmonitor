package us.dot.its.jpo.conflictmonitor.atspm.scheduler;

import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.atspm.algorithms.SpringScheduledTask;
import us.dot.its.jpo.conflictmonitor.atspm.client.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.atspm.client.AtspmTokenService;

@Component
public class AtsmpSpatValidationTask extends SpringScheduledTask {

    private final AtspmTokenService tokenService;
    private final AtspmClientService clientService;

    public AtsmpSpatValidationTask(ThreadPoolTaskScheduler taskScheduler) {
        super(taskScheduler);
    }

    @Override
    public void run() {

    }
}
