package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.BaseStreamsTopology;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.IntersectionVehicleRequestKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.SrmRequest;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedSignalRequest;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.SrmProperties;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestConstants.DEFAULT_PRIORITY_PREEMPTION_REQUEST_ALGORITHM;
import static us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuVehicleIdKey;
import static us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey;
import static us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSrm;
import static us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSsm;

@Slf4j
@Component(DEFAULT_PRIORITY_PREEMPTION_REQUEST_ALGORITHM)
public class PriorityPreemptionRequestTopology
    extends BaseStreamsTopology<PriorityPreemptionRequestParameters>
    implements PriorityPreemptionRequestStreamsAlgorithm {

    private static final String SRM_REQUEST_TABLE_STORE = "srm-table-store";

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    public Topology buildTopology() {
        var builder = new StreamsBuilder();

        // Unwrap SRM and put each request in a KTable
        KTable<IntersectionVehicleRequestKey, SrmRequest> srmRequestTable = builder
                .stream(
                    parameters.getProcessedSrmInputTopic(),
                    Consumed.with(
                            RsuVehicleIdKey(),
                            ProcessedSrm()))
                .flatMap((rsuVehicleIdKey, processedSrm) -> {

                    List<KeyValue<IntersectionVehicleRequestKey, SrmRequest>> requestList = new ArrayList<>();

                    SrmProperties properties = processedSrm.getProperties();
                    if (properties == null) {
                        log.warn("SrmProperties is null");
                        return requestList;
                    }

                    List<ProcessedSignalRequest> requests = properties.getRequests();
                    if (requests == null || requests.isEmpty()) {
                        log.warn("SrmProperties.requests list is null or empty");
                        return requestList;
                    }

                    final String vehicleId = properties.getVehicleID();
                    if (StringUtils.isBlank(properties.getVehicleID())) {
                        log.warn("SrmProperties.vehicleID is missing");
                        return requestList;
                    }

                    final ZonedDateTime dateTime = properties.getTimeStamp();
                    final long timestamp = dateTime.toInstant().toEpochMilli();

                    for (final ProcessedSignalRequest processedRequest : requests) {
                        var requestKey = new IntersectionVehicleRequestKey(vehicleId, processedRequest);
                        var request = new SrmRequest(vehicleId, timestamp, processedRequest);
                        requestList.add(new KeyValue<>(requestKey, request));
                    }

                    return requestList;
                })
                .repartition(
                        // Partition by Intersection ID
                        Repartitioned.streamPartitioner(
                            new IntersectionIdPartitioner<IntersectionVehicleRequestKey, SrmRequest>())
                )
                .toTable(
                        Materialized.<IntersectionVehicleRequestKey, SrmRequest, KeyValueStore<Bytes, byte[]>>as(SRM_REQUEST_TABLE_STORE)
                                .withKeySerde(JsonSerdes.IntersectionVehicleRequestKey())
                                .withValueSerde(JsonSerdes.SrmRequest())
                                .withLoggingDisabled()
                                .withCachingDisabled());

        // Unwrap SSM requests
        var ssmStream = builder
                .stream(
                    parameters.getProcessedSsmInputTopic(),
                    Consumed.with(
                            RsuIntersectionKey(),
                            ProcessedSsm()))
                .flatMapValues((processedSsm) -> {
                    List<Object> responseList = new ArrayList<>();
                    var statusList = processedSsm.getStatusList();
                    for (var status : statusList) {

                    }
                    return responseList;
                });




        return builder.build();
    }
}
