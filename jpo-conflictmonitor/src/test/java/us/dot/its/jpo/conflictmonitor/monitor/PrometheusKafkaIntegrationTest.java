package us.dot.its.jpo.conflictmonitor.monitor;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.OutputCaptureRule;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import us.dot.its.jpo.conflictmonitor.monitor.metrics.KafkaStreamsMetricsBinder;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Verifies stock scrapes with native Kafka producer meters and multiple live Streams applications. */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = PrometheusEndpointTest.TestApplication.class, properties = {
        "logging.level.us.dot.its.jpo.conflictmonitor.monitor.metrics=INFO"
})
@AutoConfigureMockMvc
@AutoConfigureObservability
@EmbeddedKafka(partitions = 1, topics = "metrics-input", kraft = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class PrometheusKafkaIntegrationTest {

    @Rule public TemporaryFolder stateDirectory = new TemporaryFolder();
    @Rule public OutputCaptureRule output = new OutputCaptureRule();

    @Autowired private MockMvc mockMvc;
    @Autowired private PrometheusMeterRegistry registry;
    @Autowired private KafkaStreamsMetricsBinder binder;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private EmbeddedKafkaBroker broker;

    @Test
    public void stockScrapesSurviveMultipleTopologiesAndRestart() throws Exception {
        Properties firstProperties = streamsProperties("metrics-first");
        Properties secondProperties = streamsProperties("metrics-second");
        Topology topology = topology();
        KafkaStreams first = new KafkaStreams(topology, firstProperties);
        KafkaStreams second = new KafkaStreams(topology, secondProperties);
        KafkaStreams restarted = null;
        try {
            startAndBind(first, firstProperties, "FirstTopology");
            startAndBind(second, secondProperties, "SecondTopology");
            kafkaTemplate.send("metrics-input", "key", "value").get(30, TimeUnit.SECONDS);
            awaitProcessing("FirstTopology", "metrics-first");
            awaitProcessing("SecondTopology", "metrics-second");

            assertTrue("Native Kafka producer instrumentation should be restored",
                    registry.getMeters().stream().anyMatch(meter -> meter.getId().getName().startsWith("kafka.producer.")));
            for (int i = 0; i < 3; i++) {
                assertStockScrapes();
            }

            binder.unbind("FirstTopology", firstProperties);
            first.close(Duration.ofSeconds(30));
            assertEquals(0.0, processRate("FirstTopology", "metrics-first"), 0.0);
            assertStockScrapes();

            restarted = new KafkaStreams(topology, firstProperties);
            startAndBind(restarted, firstProperties, "FirstTopology");
            kafkaTemplate.send("metrics-input", "key", "after-restart").get(30, TimeUnit.SECONDS);
            awaitProcessing("FirstTopology", "metrics-first");
            assertEquals("Restart should reuse the same five topology gauges", 5,
                    registry.getMeters().stream().filter(meter -> meter.getId().getName().startsWith("cm.streams.")
                            && "FirstTopology".equals(meter.getId().getTag("topology"))).count());
            assertStockScrapes();
            assertFalse("Registration failures require a separate diagnosis",
                    output.getAll().contains("Micrometer registration failed"));
        } finally {
            binder.unbind("FirstTopology", firstProperties);
            binder.unbind("SecondTopology", secondProperties);
            if (restarted != null) {
                restarted.close(Duration.ofSeconds(30));
            }
            first.close(Duration.ofSeconds(30));
            second.close(Duration.ofSeconds(30));
        }
    }

    private void assertStockScrapes() throws Exception {
        mockMvc.perform(get("/actuator/prometheus").accept("text/plain;version=0.0.4"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("jvm_memory_used_bytes")))
                .andExpect(content().string(containsString("process_uptime_seconds")))
                .andExpect(content().string(containsString("cm_streams_process_rate")))
                .andExpect(content().string(containsString("application_id=\"metrics-first\"")))
                .andExpect(content().string(containsString("topology=\"SecondTopology\"")))
                .andExpect(content().string(containsString("kafka_producer_")));
        mockMvc.perform(get("/actuator/prometheus").accept("application/openmetrics-text;version=1.0.0"))
                .andExpect(status().isOk()).andExpect(content().string(endsWith("# EOF\n")));
    }

    private void startAndBind(KafkaStreams streams, Properties properties, String topologyName) {
        streams.start();
        await().atMost(Duration.ofSeconds(45)).until(() -> streams.state() == KafkaStreams.State.RUNNING);
        binder.bind(topologyName, properties, streams);
    }

    private void awaitProcessing(String topologyName, String applicationId) {
        await().atMost(Duration.ofSeconds(30)).until(() -> processRate(topologyName, applicationId) > 0);
    }

    private double processRate(String topologyName, String applicationId) {
        return registry.get("cm.streams.process.rate")
                .tags("topology", topologyName, "application_id", applicationId).gauge().value();
    }

    private Properties streamsProperties(String applicationId) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.getRoot().getAbsolutePath());
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        return properties;
    }

    private Topology topology() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("metrics-input", Consumed.with(Serdes.String(), Serdes.String()))
                .foreach((key, value) -> { });
        return builder.build();
    }
}
