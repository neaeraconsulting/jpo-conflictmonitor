package us.dot.its.jpo.conflictmonitor.monitor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import us.dot.its.jpo.conflictmonitor.KafkaConfiguration;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.AlgorithmParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.StreamsTopology;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.config.ConfigParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.DefaultConfig;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.DefaultConfigMap;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.IntersectionConfig;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.IntersectionConfigMap;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.config.ConfigTopology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** HTTP and public-method coverage for AppHealthMonitor's health endpoints. */
public class AppHealthMonitorEndpointsTest {

    private final ConcurrentHashMap<String, StreamsTopology> algorithms = new ConcurrentHashMap<>();
    private AppHealthMonitor controller;
    private KafkaAdmin kafkaAdmin;
    private KafkaConfiguration kafkaConfiguration;
    private ConfigTopology configTopology;
    private MockMvc mvc;

    @Before
    public void setUp() {
        algorithms.clear();
        controller = new AppHealthMonitor();
        MonitorServiceController service = mock(MonitorServiceController.class);
        when(service.getAlgoMap()).thenReturn(algorithms);
        kafkaAdmin = mock(KafkaAdmin.class);
        kafkaConfiguration = mock(KafkaConfiguration.class);
        configTopology = mock(ConfigTopology.class);
        controller.setMonitorServiceController(service);
        controller.setKafkaAdmin(kafkaAdmin);
        controller.setKafkaConfiguration(kafkaConfiguration);
        controller.setConfigTopology(configTopology);
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    public void summaryListsEveryEndpointAgainstContextPath() throws Exception {
        mvc.perform(get("/cm/health").contextPath("/cm"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.['config/default']").value("http://localhost/cm/health/config/default"))
                .andExpect(jsonPath("$.['config/intersection']").value("http://localhost/cm/health/config/intersection"))
                .andExpect(jsonPath("$.topics").value("http://localhost/cm/health/topics"))
                .andExpect(jsonPath("$.properties").value("http://localhost/cm/health/properties"))
                .andExpect(jsonPath("$.streams").value("http://localhost/cm/health/streams"))
                .andExpect(jsonPath("$.['streams/cpu']").value("http://localhost/cm/health/streams/cpu"))
                .andExpect(jsonPath("$.['spatial-indexes']").value("http://localhost/cm/health/spatial-indexes"))
                .andExpect(jsonPath("$.['spat-window-store']").value("http://localhost/cm/health/spat-window-store"))
                .andExpect(jsonPath("$.['bsm-window-store']").value("http://localhost/cm/health/bsm-window-store"))
                .andExpect(jsonPath("$.['map-store']").value("http://localhost/cm/health/map-store"))
                .andExpect(jsonPath("$.topologies").value("http://localhost/cm/health/topologies"));
    }

    @Test
    public void parameterObjectsIncludesConfigAndOptionalAlgorithmParameters() {
        ConfigParameters config = new ConfigParameters();
        AlgorithmParameters algorithmParameters = mock(AlgorithmParameters.class);
        ExampleAlgorithmParameters algorithmConfig = new ExampleAlgorithmParameters("balanced");
        when(algorithmParameters.listParameterObjects()).thenReturn(List.of(algorithmConfig));

        controller.setConfigParams(config);
        controller.setAlgorithmParameters(null);
        assertEquals(List.of(config), controller.parameterObjects());

        controller.setAlgorithmParameters(algorithmParameters);
        List<Object> objects = controller.parameterObjects();
        assertEquals(2, objects.size());
        assertSame(config, objects.get(0));
        assertSame(algorithmConfig, objects.get(1));
    }

    @Test
    public void propertiesEndpointSerializesConfigAndAlgorithmParameters() throws Exception {
        ConfigParameters config = new ConfigParameters();
        config.setDefaultTopicName("config-default");
        AlgorithmParameters algorithmParameters = mock(AlgorithmParameters.class);
        when(algorithmParameters.listParameterObjects())
                .thenReturn(List.of(new ExampleAlgorithmParameters("balanced")));
        controller.setConfigParams(config);
        controller.setAlgorithmParameters(algorithmParameters);

        mvc.perform(get("/health/properties"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.ConfigParameters.defaultTopicName").value("config-default"))
                .andExpect(jsonPath("$.ExampleAlgorithmParameters.mode").value("balanced"));
    }

    @Test
    public void configEndpointsReturnAllMappedEntries() throws Exception {
        DefaultConfigMap defaults = new DefaultConfigMap(Map.of(
                "conflict.threshold", new DefaultConfig<>("conflict.threshold", "conflict", 12, "Integer", null, "Default threshold"),
                "conflict.enabled", new DefaultConfig<>("conflict.enabled", "conflict", true, "Boolean", null, "Enable checks")));
        IntersectionConfigMap intersections = new IntersectionConfigMap();
        intersections.putConfig(new IntersectionConfig<>("conflict.threshold", "conflict", 7, 42,
                15, "Integer", null, "Intersection threshold"));
        when(configTopology.mapDefaultConfigs()).thenReturn(defaults);
        when(configTopology.mapIntersectionConfigs()).thenReturn(intersections);

        mvc.perform(get("/health/config/default"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$['conflict.threshold'].value").value(12))
                .andExpect(jsonPath("$['conflict.threshold'].description").value("Default threshold"))
                .andExpect(jsonPath("$['conflict.enabled'].value").value(true));
        mvc.perform(get("/health/config/intersection"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$['7']['42']['conflict.threshold'].value").value(15))
                .andExpect(jsonPath("$['7']['42']['conflict.threshold'].description").value("Intersection threshold"));
    }

    @Test
    public void topicsFiltersMissingNamesAndReturnsDescriptions() throws Exception {
        when(kafkaConfiguration.getCreateTopics()).thenReturn(List.of(
                Map.of("name", "bsm-topic", "partitions", 1),
                Map.of("name", "spat-topic", "partitions", 2),
                Map.of("partitions", 1)));
        TopicDescription bsmDescription = mock(TopicDescription.class);
        when(bsmDescription.toString()).thenReturn("BSM description");
        TopicDescription spatDescription = mock(TopicDescription.class);
        when(spatDescription.toString()).thenReturn("SPaT description");
        when(kafkaAdmin.describeTopics("bsm-topic", "spat-topic"))
                .thenReturn(Map.of("bsm-topic", bsmDescription, "spat-topic", spatDescription));

        mvc.perform(get("/health/topics"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.['bsm-topic']").value("BSM description"))
                .andExpect(jsonPath("$.['spat-topic']").value("SPaT description"));
        verify(kafkaAdmin).describeTopics("bsm-topic", "spat-topic");
    }

    @Test
    public void topicsEndpointReturnsEmptyMapWhenNoTopicsExist() throws Exception {
        when(kafkaConfiguration.getCreateTopics()).thenReturn(List.of());
        when(kafkaAdmin.describeTopics()).thenReturn(Map.of());

        mvc.perform(get("/health/topics"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(content().json("{}"));
        verify(kafkaAdmin).describeTopics();
    }

    @Test
    public void streamsEndpointReportsLiveNullAndNullStateStreams() throws Exception {
        KafkaStreams running = mock(KafkaStreams.class);
        when(running.state()).thenReturn(KafkaStreams.State.RUNNING);
        KafkaStreams noState = mock(KafkaStreams.class);
        when(noState.state()).thenReturn(null);
        addAlgorithm("Running", running, null);
        addAlgorithm("NoState", noState, null);
        addAlgorithm("NotStarted", null, null);

        mvc.perform(get("/health/streams"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.Running.state").value("RUNNING"))
                .andExpect(jsonPath("$.Running.detailsUrl").value("http://localhost/health/streams/Running"))
                .andExpect(jsonPath("$.NoState.detailsUrl").value("http://localhost/health/streams/NoState"))
                .andExpect(jsonPath("$.NoState.state").doesNotExist())
                .andExpect(jsonPath("$.NotStarted.detailsUrl").value("http://localhost/health/streams/NotStarted"))
                .andExpect(jsonPath("$.NotStarted.state").doesNotExist());
    }

    @Test
    public void namedStreamsGroupsMetricsAndSupportsEmptyMetrics() throws Exception {
        KafkaStreams measured = mock(KafkaStreams.class);
        doReturn(Map.ofEntries(
                metric("records-processed", "stream-thread-metrics", 18.0),
                metric("commit-rate", "stream-thread-metrics", 2.5),
                metric("poll-latency", "consumer-metrics", 7.0))).when(measured).metrics();
        addAlgorithm("Measured", measured, null);
        KafkaStreams empty = mock(KafkaStreams.class);
        doReturn(Map.of()).when(empty).metrics();
        addAlgorithm("Empty", empty, null);

        mvc.perform(get("/health/streams/Measured"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.['stream-thread-metrics']['records-processed']").value(18.0))
                .andExpect(jsonPath("$.['stream-thread-metrics']['commit-rate']").value(2.5))
                .andExpect(jsonPath("$.['consumer-metrics']['poll-latency']").value(7.0));
        mvc.perform(get("/health/streams/Empty"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(content().json("{}"));
    }

    @Test
    public void namedStreamsReportsUnknownAndNullStreamErrors() throws Exception {
        addAlgorithm("Stopped", null, null);

        mvc.perform(get("/health/streams/missing"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.error").value("The streams map doesn't contain an object named missing"));
        mvc.perform(get("/health/streams/Stopped"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.error").value("The KafkaStreams object is null"));
    }

    @Test
    public void errorHandlerEscapesMessagesAndFallsBackForNullMessage() throws Exception {
        String message = "bad \"value\" \\ path\nnext";
        KafkaStreams escaping = mock(KafkaStreams.class);
        when(escaping.metrics()).thenThrow(new RuntimeException(message));
        addAlgorithm("Escaping", escaping, null);
        KafkaStreams noMessage = mock(KafkaStreams.class);
        when(noMessage.metrics()).thenThrow(new RuntimeException((String) null));
        addAlgorithm("NoMessage", noMessage, null);

        mvc.perform(get("/health/streams/Escaping"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.error").value(message));

        mvc.perform(get("/health/streams/NoMessage"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.error").value("unknown error"));
    }

    @Test
    public void topologyListIncludesInitializedTopologiesAndAllGraphLink() throws Exception {
        Topology initialized = topology();
        addAlgorithm("Ready", null, initialized);
        addAlgorithm("Waiting", null, null);

        mvc.perform(get("/health/topologies"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.Ready.detailsUrl").value("http://localhost/health/topologies/detail/Ready"))
                .andExpect(jsonPath("$.Ready.simpleGraphUrl").value("http://localhost/health/topologies/simple/Ready"))
                .andExpect(jsonPath("$.all.detailsUrl").value("n/a"))
                .andExpect(jsonPath("$.all.simpleGraphUrl").value("http://localhost/health/topologies/simple/all"))
                .andExpect(jsonPath("$.Waiting").doesNotExist());
    }

    @Test
    public void topologyDetailsAndDotGraphSupportNamedAndAllTopologies() throws Exception {
        addAlgorithm("Alpha", null, topology());
        addAlgorithm("Beta", null, topology());

        mvc.perform(get("/health/topologies/detail/Alpha"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("text/plain"))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("input-topic")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("output-topic")));

        mvc.perform(get("/health/topologies/simple/Alpha"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("text/plain"))
                .andExpect(header().string("Content-Disposition", "inline; filename=\"Alpha.dot\""))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Alpha")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("input_topic")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("output_topic")))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("Beta"))));

        mvc.perform(get("/health/topologies/simple/all"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("text/plain"))
                .andExpect(header().string("Content-Disposition", "inline; filename=\"all.dot\""))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Alpha")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Beta")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("input_topic")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("output_topic")));
    }

    @Test
    public void topologyEndpointsReturnUsefulErrorsForUnknownNames() throws Exception {
        mvc.perform(get("/health/topologies/detail/missing"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.error").value("The topology map doesn't contain an object named missing"));
        mvc.perform(get("/health/topologies/simple/missing"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.error").value("The topology map doesn't contain an object named missing"));
    }

    private void addAlgorithm(String name, KafkaStreams streams, Topology topology) {
        StreamsTopology algorithm = mock(StreamsTopology.class);
        when(algorithm.getStreams()).thenReturn(streams);
        when(algorithm.getTopology()).thenReturn(topology);
        algorithms.put(name, algorithm);
    }

    private Topology topology() {
        return new Topology().addSource("source", "input-topic")
                .addSink("sink", "output-topic", "source");
    }

    private Map.Entry<MetricName, Metric> metric(String name, String group, Object value) {
        Metric metric = mock(Metric.class);
        when(metric.metricValue()).thenReturn(value);
        return Map.entry(new MetricName(name, group, "test metric", Map.of()), metric);
    }

    public static class ExampleAlgorithmParameters {
        private final String mode;

        public ExampleAlgorithmParameters(String mode) {
            this.mode = mode;
        }

        public String getMode() {
            return mode;
        }
    }
}
