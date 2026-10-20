package us.dot.its.jpo.conflictmonitor.monitor.algorithms;

import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.junit.Test;
import org.mockito.InOrder;
import org.slf4j.Logger;
import org.springframework.test.util.ReflectionTestUtils;
import us.dot.its.jpo.conflictmonitor.monitor.metrics.KafkaStreamsMetricsBinder;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class BaseStreamsTopologyTest {
    @Test
    public void metricsBindAfterStartAndUnbindBeforeClose() {
        TestTopology topology = configuredTopology();
        KafkaStreamsMetricsBinder binder = mock(KafkaStreamsMetricsBinder.class);
        ReflectionTestUtils.setField(topology, "kafkaStreamsMetricsBinder", binder);
        KafkaStreams.StateListener listener = mock(KafkaStreams.StateListener.class);
        StreamsUncaughtExceptionHandler handler = mock(StreamsUncaughtExceptionHandler.class);
        topology.registerStateListener(listener);
        topology.registerUncaughtExceptionHandler(handler);
        topology.start();
        KafkaStreams streams = topology.getStreams();
        InOrder order = inOrder(streams, binder);
        order.verify(streams).setUncaughtExceptionHandler(handler);
        order.verify(streams).setStateListener(listener);
        order.verify(streams).start();
        order.verify(binder).bind("TestTopology", topology.getStreamsProperties(), streams);
        topology.stop();
        order.verify(binder).unbind("TestTopology", topology.getStreamsProperties());
        order.verify(streams).close();
        order.verify(streams).cleanUp();
        assertNull(topology.getStreams());
        topology.stop();
        verify(streams, times(1)).close();
    }

    @Test
    public void topologyCanStartAndStopWithoutMetricsBinderOrListeners() {
        TestTopology topology = configuredTopology();
        topology.start();
        KafkaStreams streams = topology.getStreams();
        verify(streams).start();
        verify(streams, never()).setStateListener(any());
        verify(streams, never()).setUncaughtExceptionHandler(any(StreamsUncaughtExceptionHandler.class));
        topology.stop();
        assertNull(topology.getStreams());
        topology.stop();
    }

    @Test
    public void startValidatesParametersPropertiesAndExistingStreams() {
        TestTopology topology = new TestTopology();
        assertThrows(IllegalStateException.class, topology::start);
        topology.setParameters(new Object());
        assertThrows(IllegalStateException.class, topology::start);
        topology.setStreamsProperties(new Properties());
        KafkaStreams streams = mock(KafkaStreams.class);
        topology.setStreams(streams);
        when(streams.state()).thenReturn(KafkaStreams.State.RUNNING);
        assertThrows(IllegalStateException.class, topology::start);
        when(streams.state()).thenReturn(KafkaStreams.State.NOT_RUNNING);
        topology.start();
        verify(topology.getStreams()).start();
        topology.stop();
    }

    @Test
    public void failedStartDoesNotBindGauges() {
        TestTopology topology = configuredTopology();
        KafkaStreamsMetricsBinder binder = mock(KafkaStreamsMetricsBinder.class);
        ReflectionTestUtils.setField(topology, "kafkaStreamsMetricsBinder", binder);
        doThrow(new IllegalStateException("cannot start")).when(topology.createdStreams).start();
        assertThrows(IllegalStateException.class, topology::start);
        verifyNoInteractions(binder);
    }

    private TestTopology configuredTopology() {
        TestTopology topology = new TestTopology();
        topology.setParameters(new Object());
        topology.setStreamsProperties(new Properties());
        return topology;
    }

    private static class TestTopology extends BaseStreamsTopology<Object> {
        private final KafkaStreams createdStreams = mock(KafkaStreams.class);
        private final Logger logger = mock(Logger.class);

        @Override protected Logger getLogger() { return logger; }
        @Override public Topology buildTopology() { return new Topology(); }
        @Override protected KafkaStreams createKafkaStreams() { return createdStreams; }
    }
}
