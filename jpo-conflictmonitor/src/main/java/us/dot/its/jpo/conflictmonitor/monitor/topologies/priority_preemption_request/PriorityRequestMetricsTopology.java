package us.dot.its.jpo.conflictmonitor.monitor.topologies.priority_preemption_request;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.BaseStreamsBuilder;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.CommonMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.IntersectionVehicleTypeKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.PriorityRequestMetrics;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsConstants.DEFAULT_PRIORITY_REQUEST_METRICS_ALGORITHM;

@Slf4j
@Component(DEFAULT_PRIORITY_REQUEST_METRICS_ALGORITHM)
public class PriorityRequestMetricsTopology
    extends BaseStreamsBuilder<PriorityRequestMetricsParameters>
    implements PriorityRequestMetricsStreamsAlgorithm {

    private CommonMetricsParameters commonMetricsParameters;

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    public void setCommonParameters(CommonMetricsParameters parameters) {
        this.commonMetricsParameters = parameters;
    }

    @Override
    public CommonMetricsParameters getCommonParameters() {
        return commonMetricsParameters;
    }

    @Override
    public KStream<IntersectionVehicleTypeKey, PriorityRequestMetrics> buildTopology(StreamsBuilder builder, KStream<IntersectionVehicleTypeKey, PriorityPreemptionRequestEvent> inputStream) {
        return null;
    }
}
