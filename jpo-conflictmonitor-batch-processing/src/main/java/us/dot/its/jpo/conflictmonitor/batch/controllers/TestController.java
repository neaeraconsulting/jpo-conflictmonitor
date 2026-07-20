package us.dot.its.jpo.conflictmonitor.batch.controllers;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;

import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import us.dot.its.jpo.conflictmonitor.batch.events.AtspmSpatSignalGroupAlignmentEvent;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmToken;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmTokenService;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm_spat_validation.AtspmSpatValidationService;
import us.dot.its.jpo.conflictmonitor.batch.services.spat.ProcessedSpatService;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.*;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedSignal;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupIndicationLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupStateLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatLog;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;


import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Health check method, and methods that pass through to the ATSPM service, for testing the API.
 * <p>
 * Disabled by default (see {@code cm.batch.test-controller.enabled}) since these endpoints
 * are unauthenticated and some (e.g. /test/token) expose live ATSPM credentials/data - only
 * enable for local development/testing, never in a shared or production environment.
 */
@ConditionalOnProperty(name = "cm.batch.test-controller.enabled", havingValue = "true", matchIfMissing = false)
@RestController
@RequestMapping(path = "/test", produces = MediaType.APPLICATION_JSON_VALUE)
@Slf4j
public class TestController {

    private final AtspmTokenService tokenService;
    private final AtspmClientService clientService;
    private final ProcessedSpatService spatService;
    private final AtspmSpatValidationService validationService;

    @Autowired
    public TestController(AtspmTokenService tokenService, AtspmClientService clientService,
                          ProcessedSpatService spatService, AtspmSpatValidationService validationService) {
        this.tokenService = tokenService;
        this.clientService = clientService;
        this.spatService = spatService;
        this.validationService = validationService;
    }

    @GetMapping(path = "/health")
    public Health health() {
        return new Health(true, String.format("%s", Instant.now()), null);
    }

    @GetMapping(path = "/token")
    public AtspmToken getToken() {
        return tokenService.token();
    }

    @GetMapping(path = "/authenticate", produces = MediaType.TEXT_PLAIN_VALUE)
    public String authenticate() {
        return clientService.authenticate();
    }

    @GetMapping(path = "/forall", produces = MediaType.TEXT_PLAIN_VALUE)
    public String forall() {
        return clientService.forall();
    }

    @GetMapping(path = "/ControllerType")
    public List<ControllerType> controllerType() {
        return clientService.controllerType();
    }

    @GetMapping(path = "/SignalConfig/{signalId}")
    public Signal signalConfig(@PathVariable String signalId) {
        return clientService.signalConfig(signalId);
    }

    @GetMapping(path = "/DirectionType")
    public List<DirectionType> directionType() {
        return clientService.directionType();
    }

    @GetMapping(path = "/LaneType")
    public List<LaneType> laneType() {
        return clientService.laneType();
    }

    @GetMapping(path = "/MovementType")
    public List<MovementType> movementType() {
        return clientService.movementType();
    }

    @GetMapping(path = "/ApproachConfig/{approachId}")
    public Approach approachConfig(@PathVariable int approachId) {
        return clientService.approachConfig(approachId);
    }

    @GetMapping(path = "/DetectorConfig/{detectorId}")
    public Detector detectorConfig(@PathVariable String detectorId) {
        return clientService.detectorConfig(detectorId);
    }

    @GetMapping(path = "/controllerEventLogs")
    public List<ControllerEventLog> controllerEventLogs(
            @RequestParam("StartTime") LocalDateTime startTime,
            @RequestParam("EndTime") LocalDateTime endTime,
            @RequestParam("RouteIds") int routeId) {
        return clientService.controllerEventLogs(startTime, endTime, routeId);

    }

    @GetMapping(path = "/processedEventLogs")
    public ProcessedControllerEventLog processedEventLogs(
            @RequestParam("StartTime") LocalDateTime startTime,
            @RequestParam("EndTime") LocalDateTime endTime,
            @RequestParam("RouteIds") int routeId) {
        return clientService.processedEventLogs(startTime, endTime, routeId);

    }

    @GetMapping(path = "/ProcessedSignal/{signalId}")
    public ProcessedSignal processedSignal(@PathVariable String signalId) {
        return clientService.processedSignalConfig(signalId);
    }

    @GetMapping(path = "/processedSpats/{intersectionId}")
    public List<ProcessedSpat> listProcessedSpats(
            @PathVariable int intersectionId,
            @RequestParam Instant startTime,
            @RequestParam Instant endTime) {
        log.info("intersectionID: {}, startTime: {}, endTime: {}", intersectionId, startTime, endTime);
        return spatService.listProcessedSpats(intersectionId, startTime, endTime);
    }

    @GetMapping(path = "/spatLogs/{intersectionId}")
    public SpatLog spatLogs(
            @PathVariable int intersectionId,
            @RequestParam Instant startTime,
            @RequestParam Instant endTime) {
        log.info("intersectionID: {}, startTime: {}, endTime: {}", intersectionId, startTime, endTime);
        return spatService.spatLogs(intersectionId, startTime, endTime);
    }

    @GetMapping(path = "/signalGroupLogs/{intersectionId}")
    public SignalGroupStateLog signalGroupLogs(
            @PathVariable int intersectionId,
            @RequestParam Instant startTime,
            @RequestParam Instant endTime) {
        log.info("intersectionID: {}, startTime: {}, endTime: {}", intersectionId, startTime, endTime);
        return spatService.signalGroupLogs(intersectionId, startTime, endTime);
    }

    @GetMapping(path = "/signalGroupIndicationLogs/{intersectionId}")
    public SignalGroupIndicationLog signalGroupIndicationLogs(
            @PathVariable int intersectionId,
            @RequestParam Instant startTime,
            @RequestParam Instant endTime) {
        log.info("intersectionID: {}, startTime: {}, endTime: {}", intersectionId, startTime, endTime);
        return spatService.signalGroupIndicationLogs(intersectionId, startTime, endTime);
    }

    @GetMapping(path = "/atspmSpatValidation/{routeId}")
    public List<AtspmSpatPairLog> atspmSpatValidation(
            @PathVariable int routeId,
            @RequestParam Instant startTime,
            @RequestParam Instant endTime) {
        log.info("routeId: {}, startTime: {}, endTime: {}", routeId, startTime, endTime);
        return validationService.atpsmSpatLogs(routeId, startTime, endTime);
    }

    @GetMapping(path = "/atspmSpatSignalGroupAlignment/{routeId}")
    public List<AtspmSpatSignalGroupAlignmentEvent> signalGroupAlignmentEvent(
            @PathVariable int routeId,
            @RequestParam Instant startTime,
            @RequestParam Instant endTime) {
        log.info("routeId: {}, startTime: {}, endTime: {}", routeId, startTime, endTime);
        return validationService.atspmSpatSignalGroupAlignmentEvents(routeId, startTime, endTime);
    }

    @ExceptionHandler(Throwable.class)
    public Health handleException(Throwable ex) {
        String rootMsg = ExceptionUtils.getRootCauseMessage(ex);
        String rootStack = ExceptionUtils.getStackTrace(ExceptionUtils.getRootCause(ex));
        String msg = String.format("%s, root cause: %s, stack trace: %s", ex.getMessage(), rootMsg, rootStack);
        return new Health(false, ex.getClass().getName(), msg);
    }



    public record Health (boolean healthy, String message, String exception){}
}
