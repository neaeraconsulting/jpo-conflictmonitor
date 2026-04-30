package us.dot.its.jpo.conflictmonitor.atspm;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Controller;
import us.dot.its.jpo.conflictmonitor.atspm.scheduler.AtsmpSpatValidationTask;

@Slf4j
@Controller
public class AtspmServiceController {

    final ThreadPoolTaskScheduler taskScheduler;
    final AtsmpSpatValidationTask atspmSpatValidationTask;

    @Autowired
    public AtspmServiceController(ThreadPoolTaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
        atspmSpatValidationTask = new AtsmpSpatValidationTask(taskScheduler);
        atspmSpatValidationTask.start();
        log.info("AtspmServiceController started.");
    }

}
