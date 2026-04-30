package us.dot.its.jpo.conflictmonitor.batch.controllers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;

import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import us.dot.its.jpo.conflictmonitor.batch.client.atspm.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.batch.client.atspm.AtspmToken;
import us.dot.its.jpo.conflictmonitor.batch.client.atspm.AtspmTokenService;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.*;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedSignal;


import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping(path = "/test", produces = MediaType.APPLICATION_JSON_VALUE)
@Slf4j
public class TestController {

    private final AtspmTokenService tokenService;
    private final AtspmClientService clientService;

    @Autowired
    public TestController(AtspmTokenService tokenService, AtspmClientService clientService) {
        this.tokenService = tokenService;
        this.clientService = clientService;
    }

    @GetMapping(path = "/token")
    public AtspmToken getToken() {
        return tokenService.token();
    }

    @GetMapping(path = "/authenticate")
    public String authenticate() {
        return clientService.authenticate();
    }

    @GetMapping(path = "/forall")
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

    @GetMapping(path = "/health")
    public Health health() {
        return new Health(true, String.format("%s", Instant.now()), null);
    }

    @GetMapping(path = "/controllerEventLogs")
    public List<ControllerEventLog> controllerEventLogs(@RequestParam("StartTime") LocalDateTime startTime,
                                                        @RequestParam("EndTime") LocalDateTime endTime,
                                                        @RequestParam("RouteIds") int routeId) {
        return clientService.controllerEventLogs(startTime, endTime, routeId);

    }

    @GetMapping(path = "/ProcessedSignal/{signalId}")
    public ProcessedSignal processedSignal(@PathVariable String signalId) {
        Signal signal = clientService.signalConfig(signalId);
        List<ControllerType> controllerTypes = clientService.controllerType();
        List<MovementType> movementTypes = clientService.movementType();
        List<LaneType> laneTypes = clientService.laneType();
        List<DirectionType> directionTypes = clientService.directionType();
        return ProcessedSignal.fromSignal(signal, controllerTypes, movementTypes, laneTypes, directionTypes);
    }

    @ExceptionHandler(Throwable.class)
    public Health handleException(Throwable ex) {
        return new Health(false, ex.getClass().getName(), ex);
    }



    public record Health (boolean healthy, String message, Throwable exception){}
}
