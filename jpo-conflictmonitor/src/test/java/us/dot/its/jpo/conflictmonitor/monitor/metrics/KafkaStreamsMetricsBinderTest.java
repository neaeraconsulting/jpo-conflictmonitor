package us.dot.its.jpo.conflictmonitor.monitor.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.After;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class KafkaStreamsMetricsBinderTest {
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final KafkaStreamsMetricsBinder binder = new KafkaStreamsMetricsBinder(registry);

    @After
    public void cleanup() {
        binder.shutdown();
        registry.close();
    }

    @Test
    public void bindUnbindAndRestartReuseGaugesAndRefreshValues() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "app");
        KafkaStreams streams = streamsWithRate(12.0);
        binder.bind("Topology", properties, streams);
        assertEquals(12.0, rate("app"), 0.0);
        binder.unbind("Missing", properties);
        binder.unbind("Topology", properties);
        assertEquals(0.0, rate("app"), 0.0);
        ReflectionTestUtils.invokeMethod(binder, "refreshAll");
        assertEquals(0.0, rate("app"), 0.0);
        KafkaStreams restarted = streamsWithRate(24.0);
        binder.bind("Topology", properties, restarted);
        assertEquals(24.0, rate("app"), 0.0);
        assertEquals(5, registry.getMeters().size());
        ReflectionTestUtils.invokeMethod(binder, "refreshAll");
        assertEquals(24.0, rate("app"), 0.0);
    }

    @Test
    public void restartingOneApplicationPreservesAnotherApplicationsGauges() {
        Properties firstProperties = new Properties();
        firstProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "first");
        Properties secondProperties = new Properties();
        secondProperties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "second");
        binder.bind("Topology", firstProperties, streamsWithRate(10.0));
        binder.bind("Topology", secondProperties, streamsWithRate(20.0));
        var firstGauge = registry.get("cm.streams.process.rate")
                .tags("topology", "Topology", "application_id", "first").gauge();
        var secondGauge = registry.get("cm.streams.process.rate")
                .tags("topology", "Topology", "application_id", "second").gauge();

        binder.unbind("Topology", firstProperties);
        assertEquals(0.0, firstGauge.value(), 0.0);
        assertEquals(20.0, secondGauge.value(), 0.0);
        binder.bind("Topology", firstProperties, streamsWithRate(30.0));
        ReflectionTestUtils.invokeMethod(binder, "refreshAll");

        assertEquals(30.0, firstGauge.value(), 0.0);
        assertEquals(20.0, secondGauge.value(), 0.0);
        assertSame(firstGauge, registry.get("cm.streams.process.rate")
                .tags("topology", "Topology", "application_id", "first").gauge());
        assertSame(secondGauge, registry.get("cm.streams.process.rate")
                .tags("topology", "Topology", "application_id", "second").gauge());
        assertEquals(10, registry.getMeters().size());
    }

    @Test
    public void missingInputsDoNotRegisterMetersAndApplicationIdDefaultsToTopology() {
        binder.bind("Topology", new Properties(), null);
        binder.bind("Topology", null, mock(KafkaStreams.class));
        binder.unbind("Topology", null);
        assertTrue(registry.getMeters().isEmpty());
        binder.bind("Topology", new Properties(), streamsWithRate(3.0));
        assertEquals(3.0, rate("Topology"), 0.0);
        binder.unbind("Topology", new Properties());
        assertEquals(0.0, rate("Topology"), 0.0);
    }

    @Test
    public void unavailableStreamsMetricsResetPreviousSample() {
        KafkaStreams streams = streamsWithRate(5.0);
        binder.bind("Topology", new Properties(), streams);
        doThrow(new IllegalStateException("closed")).when(streams).metrics();
        ReflectionTestUtils.invokeMethod(binder, "refreshAll");
        assertEquals(0.0, rate("Topology"), 0.0);
    }

    @Test
    public void failedMetricReadDoesNotPreventOtherTopologyRefreshes() {
        KafkaStreams failed = streamsWithRate(1.0);
        KafkaStreams healthy = streamsWithRate(2.0);
        binder.bind("Failed", new Properties(), failed);
        binder.bind("Topology", new Properties(), healthy);
        Metric brokenMetric = mock(Metric.class);
        when(brokenMetric.metricValue()).thenThrow(new IllegalStateException("unavailable"));
        doReturn(Map.of(new MetricName("process-rate", "stream-thread-metrics", "", Map.of()), brokenMetric))
                .when(failed).metrics();
        doReturn(metricsWithRate(7.0)).when(healthy).metrics();
        ReflectionTestUtils.invokeMethod(binder, "refreshAll");
        assertEquals(7.0, rate("Topology"), 0.0);
    }

    private double rate(String applicationId) {
        return registry.get("cm.streams.process.rate")
                .tags("topology", "Topology", "application_id", applicationId).gauge().value();
    }

    private KafkaStreams streamsWithRate(double rate) {
        KafkaStreams streams = mock(KafkaStreams.class);
        doReturn(metricsWithRate(rate)).when(streams).metrics();
        return streams;
    }

    private Map<MetricName, Metric> metricsWithRate(double rate) {
        Metric metric = mock(Metric.class);
        when(metric.metricValue()).thenReturn(rate);
        return Map.of(new MetricName("process-rate", "stream-thread-metrics", "", Map.of()), metric);
    }
}
