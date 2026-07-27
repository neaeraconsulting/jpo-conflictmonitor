package us.dot.its.jpo.conflictmonitor.batch.services.atspm;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClient;

import java.util.concurrent.atomic.AtomicReference;

@Service
@Slf4j
public class AtspmTokenServiceImpl implements AtspmTokenService {

    private final AtspmClientProperties properties;
    private final RestClient restClient;
    private final AtomicReference<AtspmToken> token;

    @Autowired
    public AtspmTokenServiceImpl(AtspmClientProperties properties,
                                 @Qualifier("tokenClient") RestClient restClient,
                                 AtomicReference<AtspmToken> token) {
        this.properties = properties;
        this.restClient = restClient;
        this.token = token;
    }

    @Override
    public AtspmToken token() {

        MultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
        formData.add("grant_type", "password");
        formData.add("username", properties.getUsername());
        formData.add("password", properties.getPassword());


        AtspmToken newToken = restClient.post()
                .uri("/token")
                .body(formData)
                .retrieve()
                .body(AtspmToken.class);

        if (newToken != null && newToken.getAccessToken() != null) {
            // Atomic swap: other threads reading the token always see either the old or the
            // new token in full, never a partially-updated one.
            token.set(newToken);
        } else {
            throw new RuntimeException("Failed to get token");
        }
        return token.get();
    }
}
