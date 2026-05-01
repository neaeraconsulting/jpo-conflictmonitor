package us.dot.its.jpo.conflictmonitor.batch.scheduler.atspm_spat_validation;

import lombok.extern.slf4j.Slf4j;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.SpringScheduledTask;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationTaskMetadata;
import us.dot.its.jpo.conflictmonitor.batch.client.atspm.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.batch.client.atspm.AtspmToken;
import us.dot.its.jpo.conflictmonitor.batch.client.atspm.AtspmTokenService;

import java.time.*;


@Slf4j

public class AtspmSpatValidationTask
        extends SpringScheduledTask<AtspmSpatValidationTaskMetadata, AtspmSpatValidationParameters> {

    private final AtspmTokenService tokenService;
    private final AtspmClientService clientService;


    public AtspmSpatValidationTask(
            AtspmSpatValidationTaskMetadata taskMetadata,
            AtspmSpatValidationParameters parameters,
            AtspmTokenService tokenService,
            AtspmClientService clientService) {
        super(taskMetadata, parameters);
        this.tokenService = tokenService;
        this.clientService = clientService;
    }

    @Override
    public void run() {
        AtspmToken token = tokenService.token();
        String authentication = clientService.authenticate();
        assert authentication != null;

    }
}
