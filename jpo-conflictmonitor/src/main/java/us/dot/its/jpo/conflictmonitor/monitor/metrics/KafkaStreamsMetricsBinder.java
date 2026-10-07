package us.dot.its.jpo.conflictmonitor.monitor.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Publishes selected Kafka Streams thread metrics as per-topology compute gauges.
 * Aggregates thread ratios and latencies and sums processing rates for each topology.
 */
@Component
public class KafkaStreamsMetricsBinder {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsMetricsBinder.class);

    private final MeterRegistry meterRegistry;
    private final ConcurrentHashMap<String, TopologyMetricSample> samples = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "cm-streams-metrics");
        t.setDaemon(true);
        return t;
    });

    public KafkaStreamsMetricsBinder(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        scheduler.scheduleAtFixedRate(this::refreshAll, 5, 15, TimeUnit.SECONDS);
    }

    public void bind(String topologyName, Properties streamsProperties, KafkaStreams streams) {
        if (streams == null || streamsProperties == null) {
            return;
        }
        String applicationId = streamsProperties.getProperty(
                StreamsConfig.APPLICATION_ID_CONFIG, topologyName);
        String key = applicationId + "|" + topologyName;

        TopologyMetricSample sample = samples.computeIfAbsent(key, k -> {
            TopologyMetricSample created = new TopologyMetricSample(topologyName, applicationId);
            registerGauges(created);
            return created;
        });
        sample.streamsRef.set(streams);
        refresh(sample);
        logger.info("Bound topology compute gauges for topology={} application.id={}",
                topologyName, applicationId);
    }

    public void unbind(String topologyName, Properties streamsProperties) {
        if (streamsProperties == null) {
            return;
        }
        String applicationId = streamsProperties.getProperty(
                StreamsConfig.APPLICATION_ID_CONFIG, topologyName);
        TopologyMetricSample sample = samples.get(applicationId + "|" + topologyName);
        if (sample != null) {
            sample.streamsRef.set(null);
            sample.reset();
        }
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
        samples.clear();
    }

    private void registerGauges(TopologyMetricSample sample) {
        Tags tags = Tags.of(
                "topology", sample.topologyName,
                "application_id", sample.applicationId);

        Gauge.builder("cm.streams.process.ratio", sample, s -> s.processRatio)
                .description("Avg Kafka Streams thread process-ratio for this topology")
                .tags(tags)
                .register(meterRegistry);
        Gauge.builder("cm.streams.process.latency.avg.ms", sample, s -> s.processLatencyAvgMs)
                .description("Avg Kafka Streams thread process-latency-avg (ms)")
                .tags(tags)
                .register(meterRegistry);
        Gauge.builder("cm.streams.process.latency.max.ms", sample, s -> s.processLatencyMaxMs)
                .description("Max Kafka Streams thread process-latency-max (ms)")
                .tags(tags)
                .register(meterRegistry);
        Gauge.builder("cm.streams.poll.ratio", sample, s -> s.pollRatio)
                .description("Avg Kafka Streams thread poll-ratio for this topology")
                .tags(tags)
                .register(meterRegistry);
        Gauge.builder("cm.streams.process.rate", sample, s -> s.processRate)
                .description("Sum of Kafka Streams thread process-rate for this topology")
                .tags(tags)
                .register(meterRegistry);
    }

    private void refreshAll() {
        for (TopologyMetricSample sample : samples.values()) {
            try {
                refresh(sample);
            } catch (Exception e) {
                logger.debug("Failed refreshing streams metrics for {}: {}",
                        sample.topologyName, e.getMessage());
            }
        }
    }

    private void refresh(TopologyMetricSample sample) {
        KafkaStreams streams = sample.streamsRef.get();
        if (streams == null) {
            sample.reset();
            return;
        }

        Map<MetricName, ? extends Metric> metrics;
        try {
            metrics = streams.metrics();
        } catch (Exception e) {
            sample.reset();
            return;
        }

        var aggregate = StreamsThreadMetrics.from(metrics);
        sample.processRatio = aggregate.processRatio();
        sample.processLatencyAvgMs = aggregate.processLatencyAvgMs();
        sample.processLatencyMaxMs = aggregate.processLatencyMaxMs();
        sample.pollRatio = aggregate.pollRatio();
        sample.processRate = aggregate.processRate();
    }

    private static final class TopologyMetricSample {
        private final String topologyName;
        private final String applicationId;
        private final AtomicReference<KafkaStreams> streamsRef = new AtomicReference<>();

        private volatile double processRatio;
        private volatile double processLatencyAvgMs;
        private volatile double processLatencyMaxMs;
        private volatile double pollRatio;
        private volatile double processRate;

        private TopologyMetricSample(String topologyName, String applicationId) {
            this.topologyName = topologyName;
            this.applicationId = applicationId;
        }

        private void reset() {
            processRatio = 0;
            processLatencyAvgMs = 0;
            processLatencyMaxMs = 0;
            pollRatio = 0;
            processRate = 0;
        }
    }
}
