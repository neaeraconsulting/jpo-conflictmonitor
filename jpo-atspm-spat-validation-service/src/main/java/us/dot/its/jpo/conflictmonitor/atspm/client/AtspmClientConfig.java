package us.dot.its.jpo.conflictmonitor.atspm.client;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class AtspmClientConfig {

    private final AtspmClientProperties properties;

    @Autowired
    public AtspmClientConfig(AtspmClientProperties properties) {
        this.properties = properties;
    }

    /**
     * Singleton to hold the current token
     * @return The token holder, initially empty
     */
    @Bean
    public AtspmToken atspmClientToken() {
        return new AtspmToken();
    }

    @Bean
    public RestClient atspmRestClient() {
        return RestClient.builder()
                .requestFactory(new HttpComponentsClientHttpRequestFactory())
                .baseUrl(properties.getBaseUrl())
                .build();
    }
}
