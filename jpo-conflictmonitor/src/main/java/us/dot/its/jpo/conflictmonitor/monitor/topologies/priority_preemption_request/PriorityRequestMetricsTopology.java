package us.dot.its.jpo.conflictmonitor.monitor.topologies.priority_preemption_request;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
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
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.IntersectionVehicleTypeKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.PriorityRequestMetrics;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.IntersectionVehicleRequestKey;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.conflictmonitor.monitor.utils.TimePeriodCalculator;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedPrioritizationResponseStatus;

import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
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
//        final long retentionTimeMillis = TimePeriodCalculator.retentionTimeMs(interval, intervalUnits, gracePeriodMs);
//        log.info("Event store retention time: {}", retentionTimeMillis);
//        final Duration retentionTime = Duration.ofMillis(retentionTimeMillis);
//        final String eventStoreName = "PriorityPreemptionEventStore";
//        final String keyStoreName = "PriorityPreemptionKeyStore";



        final var eventTopic = parameters.getInputEventTopic();

        var allStatuses = builder
                .stream(eventTopic,
                    Consumed.with(
                            JsonSerdes.IntersectionVehicleRequestKey(),
                            JsonSerdes.PriorityPreemptionRequestEvent()))
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

                // Make sure remains partitioned by intersection
                .repartition(Repartitioned.streamPartitioner(new IntersectionIdPartitioner<>()));

        // Count all
        KTable<Windowed<IntersectionVehicleTypeKey>, Long> allStatusCounts = allStatuses
                .groupByKey(Grouped.with(JsonSerdes.IntersectionVehicleTypeKey(), Serdes.String()))
                .windowedBy(
                        // TumblingWindow
                        TimeWindows.ofSizeAndGrace(tumblingWindowDuration, gracePeriodDuration)
                                .advanceBy(tumblingWindowDuration))
                .count(
                        Materialized.<IntersectionVehicleTypeKey, Long, WindowStore<Bytes, byte[]>>as("granted-counts")
                                .withKeySerde(JsonSerdes.IntersectionVehicleTypeKey())
                                .withValueSerde(Serdes.Long()))
                .suppress(untilWindowCloses(unbounded()));

        // Count granted statuses
        KTable<Windowed<IntersectionVehicleTypeKey>, Long> grantedCounts = allStatuses
                .filter((key, value) -> GRANTED.getName().equals(value))
                .groupByKey(Grouped.with(JsonSerdes.IntersectionVehicleTypeKey(), Serdes.String()))
                .windowedBy(
                        // TumblingWindow
                        TimeWindows.ofSizeAndGrace(tumblingWindowDuration, gracePeriodDuration)
                                .advanceBy(tumblingWindowDuration))
                .count(
                        Materialized.<IntersectionVehicleTypeKey, Long, WindowStore<Bytes, byte[]>>as("granted-counts")
                                .withKeySerde(JsonSerdes.IntersectionVehicleTypeKey())
                                .withValueSerde(Serdes.Long()))
                .suppress(untilWindowCloses(unbounded()));










        // Aggregate using a tumbling window with no grace period to avoid double counting





        final var metricTopic = parameters.getOutputMetricTopic();
    }
}
