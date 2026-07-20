package us.dot.its.jpo.conflictmonitor.batch.services.atspm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClient;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers AtspmTokenServiceImpl#token(): the ATSPM password-grant token exchange.
 */
@ExtendWith(MockitoExtension.class)
class AtspmTokenServiceImplTest {

    @Mock
    private AtspmClientProperties properties;
    @Mock
    private RestClient restClient;
    @Mock
    private RestClient.RequestBodyUriSpec requestBodyUriSpec;
    @Mock
    private RestClient.RequestBodySpec requestBodySpec;
    @Mock
    private RestClient.ResponseSpec responseSpec;

    private final AtspmToken token = new AtspmToken();

    private void stubTokenEndpoint(AtspmToken response) {
        when(restClient.post()).thenReturn(requestBodyUriSpec);
        when(requestBodyUriSpec.uri("/token")).thenReturn(requestBodySpec);
        when(requestBodySpec.body(any(MultiValueMap.class))).thenReturn(requestBodySpec);
        when(requestBodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.body(AtspmToken.class)).thenReturn(response);
    }

    @Test
    void tokenUpdatesTheSharedTokenOnASuccessfulResponse() {
        when(properties.getUsername()).thenReturn("user@example.com");
        when(properties.getPassword()).thenReturn("secret");
        var newToken = new AtspmToken();
        newToken.setAccessToken("abc123");
        newToken.setTokenType("Bearer");
        newToken.setExpiresIn(3600);
        stubTokenEndpoint(newToken);

        var service = new AtspmTokenServiceImpl(properties, restClient, token);
        AtspmToken result = service.token();

        assertThat(result, is(sameInstance(token)));
        assertThat(token.getAccessToken(), is("abc123"));
        assertThat(token.getTokenType(), is("Bearer"));
        assertThat(token.getExpiresIn(), is(3600L));
    }

    @Test
    void tokenSendsUsernameAndPasswordAsFormPostData() {
        when(properties.getUsername()).thenReturn("user@example.com");
        when(properties.getPassword()).thenReturn("secret");
        var newToken = new AtspmToken();
        newToken.setAccessToken("abc123");
        stubTokenEndpoint(newToken);

        var service = new AtspmTokenServiceImpl(properties, restClient, token);
        service.token();

        ArgumentCaptor<MultiValueMap<String, String>> bodyCaptor = ArgumentCaptor.forClass(MultiValueMap.class);
        verify(requestBodySpec).body(bodyCaptor.capture());
        MultiValueMap<String, String> formData = bodyCaptor.getValue();
        assertThat(formData.getFirst("grant_type"), is("password"));
        assertThat(formData.getFirst("username"), is("user@example.com"));
        assertThat(formData.getFirst("password"), is("secret"));
    }

    @Test
    void tokenThrowsWhenResponseHasNoAccessToken() {
        when(properties.getUsername()).thenReturn("user@example.com");
        when(properties.getPassword()).thenReturn("secret");
        stubTokenEndpoint(null);

        var service = new AtspmTokenServiceImpl(properties, restClient, token);

        assertThrows(RuntimeException.class, service::token);
    }
}
