package us.dot.its.jpo.conflictmonitor.monitor.metrics;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class StreamsThreadMetricsTest {

    @Test
    public void idleThreadNaNValuesAreIgnored() {
        Map<MetricName, Metric> metrics = new HashMap<>();
        put(metrics, "busy", "process-ratio", 0.6);
        put(metrics, "busy", "process-latency-avg", 4.0);
        put(metrics, "busy", "process-latency-max", 9.0);
        put(metrics, "busy", "poll-ratio", 0.2);
        put(metrics, "busy", "process-rate", 100.0);
        put(metrics, "idle", "process-ratio", 0.0);
        put(metrics, "idle", "process-latency-avg", Double.NaN);
        put(metrics, "idle", "process-latency-max", Double.NaN);
        put(metrics, "idle", "poll-ratio", 0.8);
        put(metrics, "idle", "process-rate", 0.0);

        StreamsThreadMetrics result = StreamsThreadMetrics.from(metrics);

        assertEquals(0.3, result.processRatio(), 1e-9);
        assertEquals(4.0, result.processLatencyAvgMs(), 0.0);
        assertEquals(9.0, result.processLatencyMaxMs(), 0.0);
        assertEquals(0.5, result.pollRatio(), 1e-9);
        assertEquals(100.0, result.processRate(), 0.0);
        assertEquals(2, result.threadCount());
    }

    @Test
    public void allThreadsIdleReportsZeroLatency() {
        Map<MetricName, Metric> metrics = new HashMap<>();
        put(metrics, "idle", "process-latency-avg", Double.NaN);
        put(metrics, "idle", "process-latency-max", Double.NaN);

        StreamsThreadMetrics result = StreamsThreadMetrics.from(metrics);

        assertEquals(0.0, result.processLatencyAvgMs(), 0.0);
        assertEquals(0.0, result.processLatencyMaxMs(), 0.0);
    }

    private static void put(Map<MetricName, Metric> metrics, String threadId, String name, double value) {
        Metric metric = mock(Metric.class);
        when(metric.metricValue()).thenReturn(value);
        metrics.put(new MetricName(name, "stream-thread-metrics", "", Map.of("thread-id", threadId)), metric);
    }
}
