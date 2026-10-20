package us.dot.its.jpo.conflictmonitor.monitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.StreamsTopology;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

public class AppHealthMonitorTest {
    private final ConcurrentHashMap<String, StreamsTopology> topologies = new ConcurrentHashMap<>();
    private MockMvc mvc;

    @Before
    public void setup() {
        AppHealthMonitor controller = new AppHealthMonitor();
        MonitorServiceController service = mock(MonitorServiceController.class);
        when(service.getAlgoMap()).thenReturn(topologies);
        controller.setMonitorServiceController(service);
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    public void cpuSummaryAggregatesThreadsAndRanksTopologies() throws Exception {
        KafkaStreams busy = mock(KafkaStreams.class);
        when(busy.state()).thenReturn(KafkaStreams.State.RUNNING);
        doReturn(Map.ofEntries(
                metric("process-ratio", "one", 0.4), metric("process-ratio", "two", 0.8),
                metric("process-latency-avg", "one", 4), metric("process-latency-avg", "two", 8),
                metric("process-latency-max", "one", 9), metric("process-latency-max", "two", 12),
                metric("poll-ratio", "one", 0.2), metric("poll-ratio", "two", 0.4),
                metric("process-rate", "one", 10), metric("process-rate", "two", 20),
                metric("unknown", "one", 100), metric("process-ratio", "text", "unavailable"),
                metric("process-ratio", "task", "stream-task-metrics", 100)))
                .when(busy).metrics();
        addTopology("ZBusy", busy);
        KafkaStreams idle = mock(KafkaStreams.class);
        when(idle.state()).thenReturn(KafkaStreams.State.CREATED);
        doReturn(Map.of()).when(idle).metrics();
        addTopology("AIdle", idle);
        addTopology("Stopped", null);

        mvc.perform(get("/health/streams/cpu"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$[0].name").value("ZBusy"))
                .andExpect(jsonPath("$[0].state").value("RUNNING"))
                .andExpect(jsonPath("$[0].avgProcessRatio").value(0.6000000000000001))
                .andExpect(jsonPath("$[0].avgProcessLatencyMs").value(6.0))
                .andExpect(jsonPath("$[0].maxProcessLatencyMs").value(12.0))
                .andExpect(jsonPath("$[0].avgPollRatio").value(0.30000000000000004))
                .andExpect(jsonPath("$[0].processRate").value(30.0))
                .andExpect(jsonPath("$[0].threadCount").value(2))
                .andExpect(jsonPath("$[1].name").value("AIdle"))
                .andExpect(jsonPath("$[1].threadCount").value(0))
                .andExpect(jsonPath("$[1].processRate").value(0.0))
                .andExpect(jsonPath("$[2].name").value("Stopped"))
                .andExpect(jsonPath("$[2].state").doesNotExist());
    }

    @Test
    public void cpuSummaryWithoutTopologiesReturnsEmptyArray() throws Exception {
        mvc.perform(get("/health/streams/cpu"))
                .andExpect(status().isOk()).andExpect(content().json("[]"));
    }

    private void addTopology(String name, KafkaStreams streams) {
        StreamsTopology topology = mock(StreamsTopology.class);
        when(topology.getStreams()).thenReturn(streams);
        topologies.put(name, topology);
    }

    private Map.Entry<MetricName, Metric> metric(String name, String thread, Object value) {
        return metric(name, thread, "stream-thread-metrics", value);
    }

    private Map.Entry<MetricName, Metric> metric(String name, String thread, String group, Object value) {
        Metric metric = mock(Metric.class);
        when(metric.metricValue()).thenReturn(value);
        return Map.entry(new MetricName(name, group, "", Map.of("thread-id", thread)), metric);
    }
}
