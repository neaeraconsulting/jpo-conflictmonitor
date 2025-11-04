package us.dot.its.jpo.conflictmonitor.monitor.topologies.priority_preemption_request;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.WindowStore;
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
import us.dot.its.jpo.conflictmonitor.monitor.processors.DiagnosticProcessor;
import us.dot.its.jpo.conflictmonitor.monitor.processors.metrics.TickProcessor;
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
        final Duration storeRetentionTime = Duration.of(interval * 2L, intervalUnits);
        final var gracePeriodMs = commonParameters.getGracePeriodMs();
        final Duration gracePeriodDuration = Duration.ofMillis(gracePeriodMs);
        final var eventTopic = parameters.getInputEventTopic();

        var metricsStream = builder
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
                .repartition(
                        Repartitioned
                                .with(JsonSerdes.IntersectionVehicleTypeKey(), Serdes.String())
                                .withStreamPartitioner(new IntersectionIdPartitioner<>()))

                // Insert ticks to keep stream time moving if we stop receiving events
                .process(() -> new TickProcessor<IntersectionVehicleTypeKey>(commonParameters, parameters.isDebug(), new PriorityRequestMetrics().getName()))

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

                            // Don't count ticks in the metric
                            if (TickProcessor.TICK.equals(status)) {
                                return metrics;
                            }

                            // Anything other than a tick counts towards the total
                            metrics.setNumberOfDistinctSrmRequests(metrics.getNumberOfDistinctSrmRequests() + 1);

                            // Granted goes in numerator
                            if (GRANTED.getName().equals(status)) {
                                metrics.setNumberOfGrantedSsmResponses(metrics.getNumberOfGrantedSsmResponses() + 1);
                            }

                            return metrics;
                        },
                        Named.as("priority-request-metrics-aggregation"),
                        Materialized.<IntersectionVehicleTypeKey, PriorityRequestMetrics, WindowStore<Bytes, byte[]>>as("priority-request-metrics-aggregation-store")
                                .withKeySerde(JsonSerdes.IntersectionVehicleTypeKey())
                                .withValueSerde(JsonSerdes.PriorityRequestMetrics())
                                .withRetention(storeRetentionTime)
                )
                .toStream()

                // Filter out empty events, could happen if only ticks received in a window
                .filter((key, metrics) -> metrics.getNumberOfDistinctSrmRequests() > 0)

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

        if (this.getParameters().isDebug()) {
            metricsStream.process(() -> new DiagnosticProcessor<>("Produced PriorityRequestMetrics", log));
        }

        return metricsStream;
    }
}
