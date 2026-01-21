package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.BaseStreamsBuilder;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.CommonMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation.DynamicLaneActivationMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation.DynamicLaneActivationMetricsStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.revocable_enabled_lane_alignment.RevocableLaneStatus;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation.DynamicLaneActivationMetrics;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;

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
        return null;
    }
}
