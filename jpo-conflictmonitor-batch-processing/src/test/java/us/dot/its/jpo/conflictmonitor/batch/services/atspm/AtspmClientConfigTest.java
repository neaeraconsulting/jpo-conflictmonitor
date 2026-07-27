package us.dot.its.jpo.conflictmonitor.batch.services.atspm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.RestClient;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Covers AtspmClientConfig: the base-url guard shared by both RestClient beans.
 */
@ExtendWith(MockitoExtension.class)
class AtspmClientConfigTest {

    @Mock
    private AtspmClientProperties properties;

    @Test
    void restClientThrowsWhenBaseUrlIsNullOrEmpty() {
        var config = new AtspmClientConfig(properties);
        RestClient.Builder builder = RestClient.builder();

        when(properties.getBaseUrl()).thenReturn(null);
        assertThrows(RuntimeException.class, () -> config.restClient(builder));

        when(properties.getBaseUrl()).thenReturn("   ");
        assertThrows(RuntimeException.class, () -> config.restClient(builder));
    }

    @Test
    void restClientBuildsSuccessfullyWithAValidBaseUrl() {
        when(properties.getBaseUrl()).thenReturn("https://example.com/AtspmApi");
        var config = new AtspmClientConfig(properties);

        RestClient client = config.restClient(RestClient.builder());

        assertThat(client, is(notNullValue()));
    }

    @Test
    void tokenRestClientThrowsWhenBaseUrlIsNullOrEmpty() {
        var config = new AtspmClientConfig(properties);
        RestClient.Builder builder = RestClient.builder();

        when(properties.getBaseUrl()).thenReturn(null);
        assertThrows(RuntimeException.class, () -> config.tokenRestClient(builder));

        when(properties.getBaseUrl()).thenReturn("   ");
        assertThrows(RuntimeException.class, () -> config.tokenRestClient(builder));
    }

    @Test
    void tokenRestClientBuildsSuccessfullyWithAValidBaseUrl() {
        when(properties.getBaseUrl()).thenReturn("https://example.com/AtspmApi");
        var config = new AtspmClientConfig(properties);

        RestClient client = config.tokenRestClient(RestClient.builder());

        assertThat(client, is(notNullValue()));
    }
}
