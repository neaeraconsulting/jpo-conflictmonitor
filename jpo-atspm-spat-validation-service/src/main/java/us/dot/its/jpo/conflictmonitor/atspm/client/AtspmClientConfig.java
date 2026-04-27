package us.dot.its.jpo.conflictmonitor.atspm.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Slf4j
@Configuration
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
    public RestClient restClient(RestClient.Builder builder) {
        final String baseUrl = properties.getBaseUrl();
        if (baseUrl == null || baseUrl.trim().isEmpty()) {
            throw new RuntimeException("AtspmClientConfig: baseUrl is null or empty");
        }
        return builder
                .requestFactory(new HttpComponentsClientHttpRequestFactory())
                .requestInterceptor((request, body, execution) -> {
                    log.info("Resolved URI: {}", request.getURI());
                    return execution.execute(request, body);
                })
                .baseUrl(properties.getBaseUrl())
                .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE)
                .defaultHeader(HttpHeaders.ACCEPT_ENCODING, "gzip", "deflate")
                .build();
    }

//    @Bean
//    public RestClientCustomizer restClientCustomizer() {
//        return builder ->
//                builder.baseUrl(properties.getBaseUrl())
//                    .requestFactory(new HttpComponentsClientHttpRequestFactory())
//                    .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE)
//                    .defaultHeader(HttpHeaders.ACCEPT_ENCODING, "gzip", "deflate");
//    }
}
