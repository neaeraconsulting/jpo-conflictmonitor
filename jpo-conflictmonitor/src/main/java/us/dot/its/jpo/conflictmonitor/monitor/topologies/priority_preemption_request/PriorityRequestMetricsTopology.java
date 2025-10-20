package us.dot.its.jpo.conflictmonitor.monitor.topologies.priority_preemption_request;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;
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

import java.time.Duration;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsConstants.DEFAULT_PRIORITY_REQUEST_METRICS_ALGORITHM;

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
        final var gracePeriodMs = commonParameters.getGracePeriodMs();
        final long retentionTimeMillis = TimePeriodCalculator.retentionTimeMs(interval, intervalUnits, gracePeriodMs);
        log.info("Event store retention time: {}", retentionTimeMillis);
        final Duration retentionTime = Duration.ofMillis(retentionTimeMillis);
        final String eventStoreName = "PriorityPreemptionEventStore";
        final String keyStoreName = "PriorityPreemptionKeyStore";

        final var eventStoreBuilder = Stores.versionedKeyValueStoreBuilder(
                 Stores.persistentVersionedKeyValueStore(eventStoreName, retentionTime),
                JsonSerdes.IntersectionVehicleRequestKey(),
                JsonSerdes.PriorityPreemptionRequestEvent());

        final var keyStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(keyStoreName),
                JsonSerdes.IntersectionVehicle
        );

        final var eventTopic = parameters.getInputEventTopic();





        final var metricTopic = parameters.getOutputMetricTopic();
    }
}
