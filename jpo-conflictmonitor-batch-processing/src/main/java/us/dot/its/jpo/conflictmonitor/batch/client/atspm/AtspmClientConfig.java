package us.dot.its.jpo.conflictmonitor.batch.client.atspm;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

@Slf4j
@Configuration
public class AtspmClientConfig {

    private final AtspmClientProperties properties;

    @Autowired
    public AtspmClientConfig(AtspmClientProperties properties) {
        this.properties = properties;
    }

    @Bean
    @Primary
    public RestClient restClient(RestClient.Builder builder) {
        final String baseUrl = properties.getBaseUrl();
        if (baseUrl == null || baseUrl.trim().isEmpty()) {
            throw new RuntimeException("AtspmClientConfig: baseUrl is null or empty");
        }

        // Append /api/data to base url for api calls
        UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromUriString(properties.getBaseUrl());
        final String apiDataUrl = uriBuilder.pathSegment("api", "data").build().toUriString();

        return builder
                .requestFactory(new HttpComponentsClientHttpRequestFactory())
                .requestInterceptor((request, body, execution) -> {
                    log.debug("Resolved URI: {}", request.getURI());
                    log.debug("Headers: {}", request.getHeaders());
                    return execution.execute(request, body);
                })
                .baseUrl(apiDataUrl)
                .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE)
                .defaultHeader(HttpHeaders.ACCEPT_ENCODING, "gzip", "deflate")
                .build();
    }

    @Bean
    @Qualifier("tokenClient")
    public RestClient tokenRestClient(RestClient.Builder builder) {
        final String baseUrl = properties.getBaseUrl();
        if (baseUrl == null || baseUrl.trim().isEmpty()) {
            throw new RuntimeException("AtspmClientConfig: baseUrl is null or empty");
        }
        return builder
                .requestFactory(new HttpComponentsClientHttpRequestFactory())
                .requestInterceptor((request, body, execution) -> {
                    log.debug("Resolved URI: {}", request.getURI());
                    return execution.execute(request, body);
                })
                .baseUrl(properties.getBaseUrl())
                .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .build();
    }

}
