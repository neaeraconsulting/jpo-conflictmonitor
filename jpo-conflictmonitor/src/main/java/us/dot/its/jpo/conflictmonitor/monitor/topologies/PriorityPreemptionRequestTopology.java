package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.BaseStreamsTopology;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestStreamsAlgorithm;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestConstants.DEFAULT_PRIORITY_PREEMPTION_REQUEST_ALGORITHM;
import static us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuVehicleIdKey;
import static us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSrm;
import static us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSsm;

@Slf4j
@Component(DEFAULT_PRIORITY_PREEMPTION_REQUEST_ALGORITHM)
public class PriorityPreemptionRequestTopology
    extends BaseStreamsTopology<PriorityPreemptionRequestParameters>
    implements PriorityPreemptionRequestStreamsAlgorithm {

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    public Topology buildTopology() {
        var builder = new StreamsBuilder();

        builder.stream(parameters.getProcessedSrmInputTopic(),
                Consumed.with(RsuVehicleIdKey(), ProcessedSrm()));

        return builder.build();
    }
}
