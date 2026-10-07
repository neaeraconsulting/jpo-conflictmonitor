package us.dot.its.jpo.conflictmonitor.monitor;

import java.time.Instant;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import us.dot.its.jpo.conflictmonitor.monitor.metrics.KafkaStreamsMetricsBinder;
import us.dot.its.jpo.conflictmonitor.monitor.metrics.MetricsConfiguration;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.IntersectionConfig;
import us.dot.its.jpo.conflictmonitor.monitor.models.concurrent_permissive.ConnectedLanesPairList;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.Assert.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Exercises the stock Actuator handler with production MVC configuration and no scrape filter. */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = PrometheusEndpointTest.TestApplication.class)
@AutoConfigureMockMvc
@AutoConfigureObservability
public class PrometheusEndpointTest {

    @Autowired private MockMvc mockMvc;
    @Autowired private PrometheusMeterRegistry registry;

    @Test
    public void prometheusTextUsesMvcByteArrayConversion() throws Exception {
        mockMvc.perform(get("/actuator/prometheus").accept("text/plain;version=0.0.4"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.TEXT_PLAIN))
                .andExpect(content().string(containsString("jvm_memory_used_bytes")));
    }

    @Test
    public void openMetricsUsesMvcByteArrayConversion() throws Exception {
        mockMvc.perform(get("/actuator/prometheus").accept("application/openmetrics-text;version=1.0.0"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/openmetrics-text"))
                .andExpect(content().string(containsString("jvm_memory_used_bytes")))
                .andExpect(content().string(endsWith("# EOF\n")));
    }

    @Test
    public void scrapeWithoutAcceptHeaderWorks() throws Exception {
        mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("process_uptime_seconds")));
    }

    @Test
    public void jsonActuatorEndpointsStillWork() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));
        mockMvc.perform(get("/actuator/metrics"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.names").isArray());
    }

    @Test
    public void customJsonSettingsStillApply() throws Exception {
        mockMvc.perform(get("/test/serialization"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.timestamp").value("2026-10-05T12:00:00Z"))
                .andExpect(jsonPath("$.optional").doesNotExist())
                .andExpect(jsonPath("$.empty").isMap());
    }

    @Test
    public void intersectionConfigUsesDeclaredValueType() throws Exception {
        String json = "{\"key\":\"test\",\"type\":\"" + ConnectedLanesPairList.class.getName()
                + "\",\"value\":[],\"units\":\"NONE\"}";
        mockMvc.perform(post("/test/config").contentType(MediaType.APPLICATION_JSON).content(json))
                .andExpect(status().isOk())
                .andExpect(content().string(ConnectedLanesPairList.class.getName()));
    }

    @Test
    public void resourceResponsesUseDefaultConverter() throws Exception {
        mockMvc.perform(get("/test/resource"))
                .andExpect(status().isOk()).andExpect(content().string("resource"));
    }

    @Test
    public void kafkaMetersAreNotBlanketDenied() {
        registry.counter("kafka.test.records", "client.id", "test-client").increment();
        assertNotNull(registry.find("kafka.test.records").counter());
    }

    @Configuration(proxyBeanMethods = false)
    @TestComponent
    @EnableAutoConfiguration
    @Import({WebConfig.class, MetricsConfiguration.class, KafkaStreamsMetricsBinder.class, FixtureController.class})
    static class TestApplication { }

    @RestController
    @TestComponent
    static class FixtureController {
        @GetMapping("/test/serialization")
        public SerializationFixture serialization() {
            return new SerializationFixture(Instant.parse("2026-10-05T12:00:00Z"), null, new Object());
        }

        @PostMapping("/test/config")
        public String config(@RequestBody IntersectionConfig<?> config) {
            return config.getValue().getClass().getName();
        }

        @GetMapping(value = "/test/resource", produces = MediaType.TEXT_PLAIN_VALUE)
        public ByteArrayResource resource() {
            return new ByteArrayResource("resource".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    record SerializationFixture(Instant timestamp, String optional, Object empty) { }
}
