package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression.RtcmMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.aggregation.RtcmMessageCountProgressionAggregationTopology;

import java.util.Properties;


@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class RtcmMessageCountTopologyTest {

    // Mock the aggregation subtopology tested seperately from this test
    @Mock
    RtcmMessageCountProgressionAggregationTopology mockAggregationTopology;

    @Test
    public void testRtcmMessageCountTopology() {
        Properties streamsConfig = createStreamsConfig();
        Topology topology = createTopology();
        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig)) {
            var rtcmInputTopic = driver.createInputTopic()
        }
    }

    private Properties createStreamsConfig() {
        Properties streamsConfig = new Properties();


        return streamsConfig;
    }

    private Topology createTopology() {
        var parameters = getParameters();
        var topology = new RtcmMessageCountProgressionTopology();
        topology.setParameters(parameters);
        topology.setAggregationAlgorithm(mockAggregationTopology);
        return topology.buildTopology();
    }

    private RtcmMessageCountProgressionParameters getParameters() {
        var parameters = new RtcmMessageCountProgressionParameters();
        // TODO
        return parameters;
    }
}

