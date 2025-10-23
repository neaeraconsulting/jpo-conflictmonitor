package us.dot.its.jpo.conflictmonitor.monitor.topologies.priority_preemption_request;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.BaseStreamsBuilder;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.CommonMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.ProcessingTimePeriod;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.TimestampExtractors.PriorityPreemptionRequestEventTimestampExtractor;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.IntersectionVehicleTypeKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.PriorityRequestMetrics;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.IntersectionVehicleRequestKey;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;

import java.time.Duration;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsConstants.DEFAULT_PRIORITY_REQUEST_METRICS_ALGORITHM;
import static us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedPrioritizationResponseStatus.GRANTED;
import static us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedPrioritizationResponseStatus.UNKNOWN;

@Slf4j
@Component(DEFAULT_PRIORITY_REQUEST_METRICS_ALGORITHM)
public class PriorityRequestMetricsTopology
    extends BaseStreamsBuilder<PriorityRequestMetricsParameters>
    implements PriorityRequestMetricsStreamsAlgorithm {

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
    public KStream<IntersectionVehicleTypeKey, PriorityRequestMetrics>
    buildTopology(
            StreamsBuilder builder,
            KStream<IntersectionVehicleRequestKey, PriorityPreemptionRequestEvent> inputStream) {

        final var interval = commonParameters.getInterval();
        final var intervalUnits = commonParameters.getIntervalUnits();
        final Duration tumblingWindowDuration = Duration.of(interval, intervalUnits);
        final var gracePeriodMs = commonParameters.getGracePeriodMs();
        final Duration gracePeriodDuration = Duration.ofMillis(gracePeriodMs);
        final var eventTopic = parameters.getInputEventTopic();

        KStream<IntersectionVehicleTypeKey, PriorityRequestMetrics> metricsStream = builder
                .stream(eventTopic,
                    Consumed.with(
                                JsonSerdes.IntersectionVehicleRequestKey(),
                                JsonSerdes.PriorityPreemptionRequestEvent())
                            .withTimestampExtractor(
                                new PriorityPreemptionRequestEventTimestampExtractor()))
                .filter((key, value) -> value != null)  // Remove any tombstones
                .map((key, value) -> {
                    // Re-key: use Intersection and Vehicle type
                    var newKey = new IntersectionVehicleTypeKey(key.getIntersectionId(), key.getRegion(), value.getVehicleType());
                    // We only care about counting granted vs. not-granted statuses per vehicle type and intersection,
                    // so select the status as the value.
                    // Use "unknown" type if missing to avoid creating a tombstone
                    String status = value.getStatus() != null ? value.getStatus().getName() : UNKNOWN.getName();
                    return new KeyValue<>(newKey, status);
                })

                // Make sure stream remains partitioned by intersection after rekey
                .repartition(Repartitioned.streamPartitioner(new IntersectionIdPartitioner<>()))

                // Group by key for aggregation
                .groupByKey(Grouped.with(JsonSerdes.IntersectionVehicleTypeKey(), Serdes.String()))

                // Tumbling window
                .windowedBy(
                        TimeWindows.ofSizeAndGrace(tumblingWindowDuration, gracePeriodDuration).advanceBy(tumblingWindowDuration))

                // Emit only when window closes
                // Ref on "TimeWindowedKStream.emitStrategy()" vs "KTable.suppress()":
                // https://lists.apache.org/thread/7jr91pvnyb9kn6nws1csbnfo483cw3kt
                .emitStrategy(EmitStrategy.onWindowClose())

                // Aggregate granted and other status events into a metric
                .aggregate(
                        // Initializer
                        PriorityRequestMetrics::new,
                        // Aggregator
                        (key, status, metrics) -> {
                            metrics.setKey(key);
                            metrics.setNumberOfDistinctSrmRequests(metrics.getNumberOfDistinctSrmRequests() + 1);
                            if (GRANTED.getName().equals(status)) {
                                metrics.setNumberOfGrantedSsmResponses(metrics.getNumberOfGrantedSsmResponses() + 1);
                            }
                            return metrics;
                        })
                .toStream()

                // Get the time period from the window bounds and rekey to normal key, remove window
                .map((windowedKey, value) -> {
                    IntersectionVehicleTypeKey key = windowedKey.key();
                    long startTime = windowedKey.window().start();
                    long endTime = windowedKey.window().end();
                    value.setKey(key);
                    ProcessingTimePeriod period = new ProcessingTimePeriod(startTime, endTime);
                    value.setTimePeriod(period);
                    return new KeyValue<>(key, value);
                });

        return metricsStream;
    }
}
