package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.EmitStrategy;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.BaseStreamsBuilder;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.CommonMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation.DynamicLaneActivationMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation.DynamicLaneActivationMetricsStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.revocable_enabled_lane_alignment.RevocableLaneStatus;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation.DynamicLaneActivationMetrics;
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

        var metricsStream = inputStream

                // Insert ticks to keep stream time moving if we stop receiving events
                .process(() -> new DynamicLaneActivationMetricsTickProcessor(commonParameters, parameters.isDebug(), timestampStoreName))

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




    }
}
