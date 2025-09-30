package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.BaseStreamsTopology;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.IntersectionVehicleRequestKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.JoinedRequestStatus;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.SrmRequest;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.SsmStatus;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedSignalRequest;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.SrmProperties;
import us.dot.its.jpo.geojsonconverter.pojos.ssm.ProcessedSignalStatus;

import java.time.Duration;
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

        final String requestStoreName = parameters.getSrmStoreName();
        final Duration retentionTime = Duration.ofMinutes(parameters.getSrmStoreRetentionTimeMinutes());
        final Duration ssmStreamGracePeriodSeconds = Duration.ofSeconds(parameters.getSsmStreamGracePeriodSeconds());





        // Unwrap SRM and put each request in a KTable to store the latest request with a given
        // intersectionId, region, vehicleId, and requestId
        KTable<IntersectionVehicleRequestKey, SrmRequest> srmRequestTable = builder
                .stream(
                    parameters.getProcessedSrmInputTopic(),
                    Consumed.with(
                                RsuVehicleIdKey(),
                                ProcessedSrm())
                            .withTimestampExtractor(new TimestampExtractor() {
                                @Override
                                public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
                                    return 0;
                                }
                            }))
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
                    // Use versioned state store for the SRM table to be able to deal with out-of-order messages easily
                    // and automatically remove old entries after the retention time
                    Materialized.<IntersectionVehicleRequestKey, SrmRequest>as(
                        Stores.persistentVersionedKeyValueStore(
                                requestStoreName,
                                retentionTime))
                            .withKeySerde(JsonSerdes.IntersectionVehicleRequestKey())
                            .withValueSerde(JsonSerdes.SrmRequest())
                );

        // Unwrap SSM requests
        KStream<IntersectionVehicleRequestKey, SsmStatus> ssmStatusStream = builder
                .stream(
                    parameters.getProcessedSsmInputTopic(),
                    Consumed.with(
                            RsuIntersectionKey(),
                            ProcessedSsm())
                            .withTimestampExtractor(new TimestampExtractor() {
                                @Override
                                public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
                                    return 0;
                                }
                            }))
                .flatMap((rsuIntersectionKey,processedSsm) -> {
                    final Integer intersectionId = processedSsm.getIntersectionId();
                    final Integer region = processedSsm.getRegion();
                    final ZonedDateTime dateTime = processedSsm.getTimeStamp();
                    final long timestamp = dateTime.toInstant().toEpochMilli();
                    List<KeyValue<IntersectionVehicleRequestKey, SsmStatus>> responseList = new ArrayList<>();
                    List<ProcessedSignalStatus> statusList = processedSsm.getStatusList();
                    for (ProcessedSignalStatus status : statusList) {
                        var key = new IntersectionVehicleRequestKey(intersectionId, region, status);
                        var ssmStatus = new SsmStatus(intersectionId, region, timestamp, status);
                        responseList.add(new KeyValue<>(key, ssmStatus));
                    }
                    return responseList;
                })
                .repartition(
                        // Partition by Intersection ID
                        Repartitioned.streamPartitioner(
                                new IntersectionIdPartitioner<IntersectionVehicleRequestKey, SsmStatus>())
                );

        ssmStatusStream.leftJoin(srmRequestTable,
                new ValueJoinerWithKey<IntersectionVehicleRequestKey, SsmStatus, SrmRequest, JoinedRequestStatus>() {
                    @Override
                    public JoinedRequestStatus apply(final IntersectionVehicleRequestKey intersectionVehicleRequestKey, SsmStatus ssmStatus, SrmRequest srmRequest) {
                        return new JoinedRequestStatus(srmRequest, ssmStatus);
                    }
                },
                Joined.as("joined-request-status").withGracePeriod());





        return builder.build();
    }
}
