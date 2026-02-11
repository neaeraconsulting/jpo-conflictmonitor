package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.BaseStreamsBuilder;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.CommonMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation.DynamicLaneActivationMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation.DynamicLaneActivationMetricsStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.revocable_enabled_lane_alignment.RevocableLaneStatus;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation.*;
import us.dot.its.jpo.conflictmonitor.monitor.processors.DiagnosticProcessor;
import us.dot.its.jpo.conflictmonitor.monitor.processors.DynamicLaneActivationMetricsTickProcessor;
import us.dot.its.jpo.conflictmonitor.monitor.processors.metrics.TickProcessor;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;


import java.time.Duration;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation.DynamicLaneActivationMetricsConstants.DEFAULT_DYNAMIC_LANE_ACTIVATION_METRICS_ALGORITHM;

@Slf4j
@Component(DEFAULT_DYNAMIC_LANE_ACTIVATION_METRICS_ALGORITHM)
public class DynamicLaneActivationMetricsTopology
        extends BaseStreamsBuilder<DynamicLaneActivationMetricsParameters>
        implements DynamicLaneActivationMetricsStreamsAlgorithm {

    private CommonMetricsParameters commonParameters;

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    public void setCommonParameters(CommonMetricsParameters parameters) {
        this.commonParameters = parameters;
    }

    @Override
    public CommonMetricsParameters getCommonParameters() {
        return commonParameters;
    }

    @Override
    public KStream<RsuIntersectionKey, DynamicLaneActivationMetrics>
    buildTopology(StreamsBuilder builder, KStream<RsuIntersectionKey, RevocableLaneStatus> inputStream) {
        final var interval = commonParameters.getInterval();
        final var intervalUnits = commonParameters.getIntervalUnits();
        final Duration tumblingWindowDuration = Duration.of(interval, intervalUnits);
        final Duration storeRetentionTime = Duration.of(interval * 2L, intervalUnits);
        final var gracePeriodMs = commonParameters.getGracePeriodMs();
        final Duration gracePeriodDuration = Duration.ofMillis(gracePeriodMs);
        final String timestampStoreName = "revocableLaneStatusTimestampStore";

        final var timestampStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(timestampStoreName),
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                        TickProcessor.TimestampsSerdes());
        builder.addStateStore(timestampStoreBuilder);

        KStream<RsuIntersectionKey, DynamicLaneActivationMetrics> metricsStream = inputStream

                // Insert ticks to keep stream time moving if we stop receiving events
                .process(() ->
                        new DynamicLaneActivationMetricsTickProcessor(commonParameters, parameters.isDebug(),
                                timestampStoreName),
                        timestampStoreName)

                // Group by key for aggregation
                .groupByKey(Grouped.with(
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                        JsonSerdes.RevocableLaneStatus()))

                // Tumbling window
                .windowedBy(TimeWindows
                        .ofSizeAndGrace(tumblingWindowDuration, gracePeriodDuration)
                        .advanceBy(tumblingWindowDuration))

                // Emit only when window closes
                .emitStrategy(EmitStrategy.onWindowClose())

                // Aggregate and deduplicate
                .aggregate(
                        // Initializer
                        DynamicLaneActivationMetrics::new,

                        // Aggregator
                        (key, status, metrics) -> {
                            metrics.setKey(key);

                            // Don't count tombstones or ticks in the metric
                            if (status == null || status.isTick()) {
                                if (parameters.isDebug()) {
                                    log.debug("Ignore TICK in metric aggregation: {}", metrics);
                                }
                                return metrics;
                            }

                            final long timestamp = status.getTimestamp();

                            RevocableEnabledLaneStatusTable table = metrics.getRevocableEnabledLaneStatusTable();
                            for (final int laneId : status.getRevocableLaneList()) {
                                final boolean enabled = status.getEnabledLaneList().contains(laneId);

                                RevocableEnabledLaneStatusChanges statusChanges = table.getChangesForLaneID(laneId);
                                if (statusChanges == null) {
                                    statusChanges = new RevocableEnabledLaneStatusChanges();
                                    statusChanges.setLaneID(laneId);
                                    table.add(statusChanges);
                                }
                                RevocableEnabledStatusList statusList = statusChanges.getStatusChanges();
                                if (statusList.isEmpty()) {
                                    // First status for this lane in the window
                                    statusList.add(new RevocableEnabledStatus(timestamp, enabled));
                                    continue;
                                }

                                final RevocableEnabledStatus lastStatus = statusList.getLast();
                                if (lastStatus != null && timestamp > lastStatus.timestamp() && enabled != lastStatus.enabled()) {
                                    statusList.add(new RevocableEnabledStatus(timestamp, enabled));
                                }
                            }

                            if (parameters.isDebug()) {
                                log.debug("Updated aggregated metrics: {}", metrics);
                            }

                            return metrics;
                        },
                        Named.as("dynamic-lane-activation-metrics-aggregation"),
                        Materialized.<RsuIntersectionKey, DynamicLaneActivationMetrics, WindowStore<Bytes, byte[]>>as("dynamic-lane-activation-metricsd-aggregation-store")
                                .withKeySerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey())
                                .withValueSerde(JsonSerdes.DynamicLaneActivationMetrics())
                                .withRetention(storeRetentionTime)
                )
                .toStream()

                // Filter out empty events that may happen due to ticks?
                // Don't necessarily filter those out so we can be alerted if changes stop
//                .filter((key, metrics) -> {
//                    // Check if there are any entries for any lanes
//                    var table = metrics.getRevocableEnabledLaneStatusTable();
//                    for (var item : table) {
//                        if (!item.getStatusChanges().isEmpty()) {
//                            return true;
//                        }
//                    }
//                    return false;
//                })

                // Get the time period from the window bounds and rekey normal key, not windowed
                .map((windowedKey, value) -> {
                    RsuIntersectionKey key = windowedKey.key();
                    long startTime = windowedKey.window().start();
                    long endTime = windowedKey.window().end();
                    value.setKey(key);
                    ProcessingTimePeriod period = new ProcessingTimePeriod(startTime, endTime);
                    value.setTimePeriod(period);
                    return new KeyValue<>(key, value);
                });

        if (getParameters().isDebug()) {
            metricsStream.process(() -> new DiagnosticProcessor<>("Produced DynamicLaneActivationMetrics", log));
        }

        return metricsStream;
    }


}
