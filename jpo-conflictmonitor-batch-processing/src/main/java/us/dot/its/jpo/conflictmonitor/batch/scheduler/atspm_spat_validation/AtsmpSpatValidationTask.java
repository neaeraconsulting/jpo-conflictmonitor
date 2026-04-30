package us.dot.its.jpo.conflictmonitor.batch.scheduler.atspm_spat_validation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.SpringScheduledTask;
import us.dot.its.jpo.conflictmonitor.batch.client.atspm.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.batch.client.atspm.AtspmTokenService;

@Component
public class AtsmpSpatValidationTask extends SpringScheduledTask {

    private final AtspmTokenService tokenService;
    private final AtspmClientService clientService;

    @Autowired
    public AtsmpSpatValidationTask(
            ThreadPoolTaskScheduler taskScheduler, AtspmTokenService tokenService,
            AtspmClientService clientService) {
        super(taskScheduler);
        this.tokenService = tokenService;
        this.clientService = clientService;
    }

    @Override
    public void run() {

    }
}
