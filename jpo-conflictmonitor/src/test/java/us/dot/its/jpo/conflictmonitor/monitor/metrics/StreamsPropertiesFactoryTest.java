package us.dot.its.jpo.conflictmonitor.monitor.metrics;

import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.ConflictMonitorProperties;

import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamsPropertiesFactoryTest {

    @Test
    public void usesDiscoveredPartitionsWhenConfiguredThreadsIsZero() {
        ConflictMonitorProperties props = mock(ConflictMonitorProperties.class);
        when(props.getStreamsConfigNumStreamThreads()).thenReturn(0);
        when(props.createStreamProperties(eq("mapSpatAlignment"))).thenReturn(baseProps("mapSpatAlignment"));

        TopicPartitionCounts counts = mock(TopicPartitionCounts.class);
        when(counts.maxPartitions(any(String[].class))).thenReturn(15);

        StreamsPropertiesFactory factory = new StreamsPropertiesFactory(props, counts);
        Properties result = factory.create("mapSpatAlignment", "topic.ProcessedSpat", "topic.ProcessedMap");

        assertThat(result.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG), equalTo(15));
    }

    @Test
    public void usesExplicitOverrideWhenConfiguredThreadsPositive() {
        ConflictMonitorProperties props = mock(ConflictMonitorProperties.class);
        when(props.getStreamsConfigNumStreamThreads()).thenReturn(4);
        when(props.createStreamProperties(eq("spatValidation"))).thenReturn(baseProps("spatValidation"));

        TopicPartitionCounts counts = mock(TopicPartitionCounts.class);
        when(counts.maxPartitions(any(String[].class))).thenReturn(15);

        StreamsPropertiesFactory factory = new StreamsPropertiesFactory(props, counts);
        Properties result = factory.create("spatValidation", "topic.ProcessedSpat");

        assertThat(result.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG), equalTo(4));
    }

    @Test
    public void defaultsToOneWhenDiscoveryFails() {
        ConflictMonitorProperties props = mock(ConflictMonitorProperties.class);
        when(props.getStreamsConfigNumStreamThreads()).thenReturn(0);
        when(props.createStreamProperties(eq("config"))).thenReturn(baseProps("config"));

        TopicPartitionCounts counts = mock(TopicPartitionCounts.class);
        when(counts.maxPartitions(any(String[].class))).thenReturn(0);

        StreamsPropertiesFactory factory = new StreamsPropertiesFactory(props, counts);
        Properties result = factory.create("config", "topic.Missing");

        assertThat(result.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG), equalTo(1));
    }

    private static Properties baseProps(String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        return props;
    }
}
