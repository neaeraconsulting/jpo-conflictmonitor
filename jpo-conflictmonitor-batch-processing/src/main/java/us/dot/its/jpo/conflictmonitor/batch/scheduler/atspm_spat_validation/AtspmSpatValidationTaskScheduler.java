package us.dot.its.jpo.conflictmonitor.batch.scheduler.atspm_spat_validation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.util.ErrorHandler;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.SpringTaskScheduler;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.*;
import us.dot.its.jpo.conflictmonitor.batch.mongo.ProcessedSpatCollectionUpdater;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmTokenService;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm_spat_validation.AtspmSpatValidationService;
import us.dot.its.jpo.conflictmonitor.batch.services.spat.ProcessedSpatService;


import java.time.Clock;

import static us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationConstants.DEFAULT_ATSPM_SPAT_VALIDATION_ALGORITHM;

@Slf4j
@Component(DEFAULT_ATSPM_SPAT_VALIDATION_ALGORITHM)
public class AtspmSpatValidationTaskScheduler
        extends SpringTaskScheduler<AtspmSpatValidationParameters, RouteConfig, AtspmSpatValidationTask>
        implements AtspmSpatValidationScheduledTaskAlgorithm {

    private final AtspmTokenService tokenService;
    private final AtspmClientService clientService;
    private final MongoTemplate mongoTemplate;
    private final AtspmSpatValidationService atspmSpatValidationService;
    private final ProcessedSpatService spatService;
    private final ProcessedSpatCollectionUpdater spatViewUpdater;

    @Autowired
    public AtspmSpatValidationTaskScheduler(ThreadPoolTaskScheduler taskScheduler, Clock clock,
                                            AtspmSpatValidationParameters parameters, AtspmTokenService tokenService,
                                            AtspmClientService clientService, MongoTemplate mongoTemplate,
                                            AtspmSpatValidationService atspmSpatValidationService,
                                            ProcessedSpatService spatService,
                                            ProcessedSpatCollectionUpdater spatViewUpdater,
                                            ErrorHandler errorHandler) {
        super(taskScheduler, clock, errorHandler);
        this.interval = parameters.getInterval();
        this.intervalUnits = parameters.getIntervalUnits();
        this.taskStartTimeStagger = parameters.getTaskStartTimeStagger();
        this.taskStartTimeStaggerUnits = parameters.getTaskStartTimeStaggerUnits();
        this.gracePeriodOffset = parameters.getGracePeriodOffset();
        this.gracePeriodOffsetUnits = parameters.getGracePeriodOffsetUnits();
        this.parameters = parameters;
        this.taskMetadata = parameters.getRoutes();
        this.tokenService = tokenService;
        this.clientService = clientService;
        this.mongoTemplate = mongoTemplate;
        this.atspmSpatValidationService = atspmSpatValidationService;
        this.spatService = spatService;
        this.spatViewUpdater = spatViewUpdater;
    }

    @Override
    protected AtspmSpatValidationTask createTask(
            RouteConfig routeConfig, AtspmSpatValidationParameters atspmSpatValidationParameters) {
        return new AtspmSpatValidationTask(routeConfig, parameters, tokenService, clientService, clock, mongoTemplate,
                atspmSpatValidationService, spatService, spatViewUpdater);
    }



}
