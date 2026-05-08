package us.dot.its.jpo.conflictmonitor.batch.services.atspm;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedSignal;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.*;
import us.dot.its.jpo.conflictmonitor.batch.time.TimeUtil;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
@Slf4j
public class AtspmClientServiceImpl implements AtspmClientService {

    private final RestClient restClient;
    private final AtspmToken token;
    private final Clock clock;
    private final AtspmSpatValidationParameters parameters;

    @Autowired
    public AtspmClientServiceImpl(RestClient restClient, AtspmToken token, Clock clock, AtspmSpatValidationParameters parameters) {
        this.restClient = restClient;
        this.token = token;
        this.clock = clock;
        this.parameters = parameters;
    }

    @Override
    public String authenticate() {
        return retrieveString("/authenticate");
    }

    @Override
    public String forall() {
        return retrieveString("/forall");
    }

    @Override
    public List<ControllerType> controllerType() {
        var returnTypeRef = new ParameterizedTypeReference<List<ControllerType>>() {};
        return retrieveList(returnTypeRef, "/ControllerType");
    }

    @Override
    public List<DirectionType> directionType() {
        var returnTypeRef = new ParameterizedTypeReference<List<DirectionType>>() {};
        return retrieveList(returnTypeRef, "/DirectionType");
    }

    @Override
    public List<LaneType> laneType() {
        var returnTypeRef = new ParameterizedTypeReference<List<LaneType>>() {};
        return retrieveList(returnTypeRef, "/LaneType");
    }

    @Override
    public List<MovementType> movementType() {
        var returnTypeRef = new ParameterizedTypeReference<List<MovementType>>() {};
        return retrieveList(returnTypeRef, "/MovementType");
    }

    @Override
    public Signal signalConfig(String signalId) {
        return retrieveObject(Signal.class, "/SignalConfig/{signalId}", signalId);
    }

    @Override
    public ProcessedSignal processedSignalConfig(String signalId) {
        Signal signal = signalConfig(signalId);
        List<ControllerType> controllerTypes = controllerType();
        List<MovementType> movementTypes = movementType();
        List<LaneType> laneTypes = laneType();
        List<DirectionType> directionTypes = directionType();
        return ProcessedSignal.fromSignal(signal, controllerTypes, movementTypes, laneTypes, directionTypes);
    }

    @Override
    public Approach approachConfig(int approachId) {
        return retrieveObject(Approach.class, "/ApproachConfig/{approachId}", approachId);
    }

    @Override
    public Detector detectorConfig(String detectorId) {
        return retrieveObject(Detector.class, "/DetectorConfig/{detectorId}", detectorId);
    }

    @Override
    public List<ControllerEventLog> controllerEventLogs(LocalDateTime startTime, LocalDateTime endTime, int routeId) {
        ParameterizedTypeReference<List<ControllerEventLog>> returnTypeRef = new  ParameterizedTypeReference<>() {};
        return retrieveList(returnTypeRef,
                "/controllerEventLogs?StartTime={startTime}&EndTime={endTime}&RouteIds={routeId}",
                localTimeFormat.format(startTime), localTimeFormat.format(endTime), routeId);
    }

    @Override
    public ProcessedControllerEventLog processedEventLogs(LocalDateTime startTime, LocalDateTime endTime, int routeId) {
        List<ControllerEventLog> eventLogs = controllerEventLogs(startTime, endTime, routeId);
        log.info("Got eventLogs with {} items", eventLogs.size());
        Instant startInstant = TimeUtil.localTimeToInstantAtZone(startTime, parameters.getLocalTimeZone(), clock);
        Instant endInstant = TimeUtil.localTimeToInstantAtZone(endTime, parameters.getLocalTimeZone(), clock);
        var atspmLog = new ProcessedControllerEventLog(routeId, startInstant, endInstant, eventLogs, clock, parameters.getLocalTimeZone());
        log.info("Got {} processed log items", atspmLog.size());
        return atspmLog;
    }

    private final static DateTimeFormatter localTimeFormat =  DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private String retrieveString(String path) {
        return restClient.get()
                .uri(path)
                .headers(headers -> headers.setBearerAuth(token.getAccessToken()))
                .retrieve()
                .body(String.class);
    }

    private <T> List<T> retrieveList(ParameterizedTypeReference<List<T>> returnTypeRef, String path, Object ... args) {
        return restClient.get()
                .uri(path, args)
                .headers(headers -> headers.setBearerAuth(token.getAccessToken()))
                .retrieve()
                .body(returnTypeRef);
    }

    private <T> T retrieveObject(Class<T> objectClass, String path, Object... args) {
        return restClient.get()
                .uri(path, args)
                .headers(headers -> headers.setBearerAuth(token.getAccessToken()))
                .retrieve()
                .body(objectClass);
    }

}
