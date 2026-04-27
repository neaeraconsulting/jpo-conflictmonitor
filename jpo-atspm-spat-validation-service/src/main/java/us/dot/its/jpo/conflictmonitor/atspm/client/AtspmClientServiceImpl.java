package us.dot.its.jpo.conflictmonitor.atspm.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import us.dot.its.jpo.conflictmonitor.atspm.models.ControllerEventLog;
import us.dot.its.jpo.conflictmonitor.atspm.models.ControllerType;
import us.dot.its.jpo.conflictmonitor.atspm.models.Signal;

import java.time.Instant;
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
        return restClient.get().uri("/api/data/authenticate")
                .headers(headers -> headers.setBearerAuth(token.getAccessToken()))
                .retrieve().body(String.class);
    }

    @Override
    public String forall() {
        return restClient.get().uri("/api/data/forall")
                .headers(headers -> headers.setBearerAuth(token.getAccessToken()))
                .retrieve().body(String.class);
    }

    @Override
    public List<Signal> signalConfig(int signalId) {
        return List.of();
    }

    @Override
    public List<ControllerType> controllerType() {
        return List.of();
    }

    @Override
    public List<ControllerEventLog> controllerEventLogs(Instant startTime, Instant endTime, int routeId) {
        return List.of();
    }
}
