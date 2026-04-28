package us.dot.its.jpo.conflictmonitor.atspm.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import us.dot.its.jpo.conflictmonitor.atspm.models.ControllerEventLog;
import us.dot.its.jpo.conflictmonitor.atspm.models.ControllerType;
import us.dot.its.jpo.conflictmonitor.atspm.models.Signal;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
@Slf4j
public class AtspmClientServiceImpl implements AtspmClientService {

    private final RestClient restClient;
    private final AtspmToken token;

    public AtspmClientServiceImpl(RestClient restClient, AtspmToken token) {
        this.restClient = restClient;
        this.token = token;
    }



    @Override
    public String authenticate() {
        return restClient.get()
                .uri("/authenticate")
                .headers(headers -> headers.setBearerAuth(token.getAccessToken()))
                .retrieve()
                .body(String.class);
    }

    @Override
    public String forall() {
        return restClient.get()
                .uri("/forall")
                .headers(headers -> headers.setBearerAuth(token.getAccessToken()))
                .retrieve()
                .body(String.class);
    }

    @Override
    public List<Signal> signalConfig(String signalId) {
        ParameterizedTypeReference<List<Signal>> returnTypeRef = new  ParameterizedTypeReference<>() {};
        return restClient.get()
                .uri("/SignalConfig/{signalId}", signalId)
                .headers(headers -> headers.setBearerAuth(token.getAccessToken()))
                .retrieve()
                .body(returnTypeRef);
    }

    @Override
    public List<ControllerType> controllerType() {
        ParameterizedTypeReference<List<ControllerType>> returnTypeRef = new ParameterizedTypeReference<>() {};
        return restClient.get()
                .uri("/ControllerType")
                .headers(headers -> headers.setBearerAuth(token.getAccessToken()))
                .retrieve()
                .body(returnTypeRef);
    }

    private final static DateTimeFormatter localTimeFormat =  DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    @Override
    public List<ControllerEventLog> controllerEventLogs(LocalDateTime startTime, LocalDateTime endTime, int routeId) {
        ParameterizedTypeReference<List<ControllerEventLog>> returnTypeRef = new  ParameterizedTypeReference<>() {};
        return restClient.get()
                .uri(builder -> builder
                        .path("/SignalConfig")
                        .queryParam("StartTime",  localTimeFormat.format(startTime))
                        .queryParam("EndTime",  localTimeFormat.format(endTime))
                        .queryParam("RouteId", routeId)
                        .build())
                .headers(headers -> headers.setBearerAuth(token.getAccessToken()))
                .retrieve()
                .body(returnTypeRef);
    }
}
