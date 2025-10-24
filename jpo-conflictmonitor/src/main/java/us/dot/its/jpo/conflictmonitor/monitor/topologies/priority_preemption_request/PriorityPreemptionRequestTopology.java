package us.dot.its.jpo.conflictmonitor.monitor.topologies.priority_preemption_request;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.BaseStreamsTopology;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request.PriorityPreemptionRequestStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.IntersectionVehicleTypeKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.PriorityRequestMetrics;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.*;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedBasicVehicleRole;
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

    private PriorityRequestMetricsStreamsAlgorithm priorityRequestMetricsStreamsAlgorithm;

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    public Topology buildTopology() {
        var builder = new StreamsBuilder();

        final String requestStoreName = parameters.getSrmStoreName();
        final Duration retentionTime = Duration.ofMinutes(parameters.getSrmStoreRetentionTimeMinutes());
        final Duration ssmStreamGracePeriod = Duration.ofMillis(parameters.getSsmStreamGracePeriodMilliseconds());


        // Unwrap SRM Requests
        KStream<IntersectionVehicleRequestKey, SrmRequest> srmRequestStream = builder
                .stream(
                    parameters.getProcessedSrmInputTopic(),
                    Consumed.with(
                                RsuVehicleIdKey(),
                                ProcessedSrm())
                            .withTimestampExtractor(new ProcessedSrmTimestampExtractor()))
                .flatMap((rsuVehicleIdKey, processedSrm) -> {
                    log.info("received SRM: {}, {}", rsuVehicleIdKey, processedSrm);
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

                    final ProcessedBasicVehicleRole vehicleType = properties.getRole();

                    final ZonedDateTime dateTime = properties.getTimeStamp();
                    final long timestamp = dateTime.toInstant().toEpochMilli();

                    for (final ProcessedSignalRequest processedRequest : requests) {
                        var requestKey = new IntersectionVehicleRequestKey(vehicleId, processedRequest);
                        var request = new SrmRequest(vehicleId, vehicleType, timestamp, processedRequest);
                        requestList.add(new KeyValue<>(requestKey, request));
                    }

                    return requestList;
                })
                .repartition(
                        // Partition by Intersection ID
                        Repartitioned
                                .streamPartitioner(new IntersectionIdPartitioner<IntersectionVehicleRequestKey, SrmRequest>())
                                .withKeySerde(JsonSerdes.IntersectionVehicleRequestKey())
                                .withValueSerde(JsonSerdes.SrmRequest())
                )
                .peek((key, value) -> {
                    log.info("SrmRequest: {}, {}", key, value);
                });




        // Put each SRM request in a KTable to store the latest request with a given
        // intersectionId, region, vehicleId, and requestId
        KTable<IntersectionVehicleRequestKey, SrmRequest> srmRequestTable =
                srmRequestStream.toTable(
                    // Use versioned state store for the SRM table to be able to deal with out-of-order messages easily
                    // and automatically remove old entries after the retention time
                    Materialized.<IntersectionVehicleRequestKey, SrmRequest>as(
                        Stores.persistentVersionedKeyValueStore(
                                requestStoreName,
                                retentionTime))
                            .withKeySerde(JsonSerdes.IntersectionVehicleRequestKey())
                            .withValueSerde(JsonSerdes.SrmRequest())
                );

        srmRequestTable.toStream().peek((key, value) -> {
           log.info("srmRequestTable: {}, {}", key, value);
        });

        // Unwrap SSM requests
        KStream<IntersectionVehicleRequestKey, SsmStatus> ssmStatusStream = builder
                .stream(
                    parameters.getProcessedSsmInputTopic(),
                    Consumed.with(
                            RsuIntersectionKey(),
                            ProcessedSsm())
                            .withTimestampExtractor(new ProcessedSsmTimestampExtractor()))
                .flatMap((rsuIntersectionKey,processedSsm) -> {
                    log.info("received SSM: {}, {}", rsuIntersectionKey, processedSsm);
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
                        Repartitioned.streamPartitioner(new IntersectionIdPartitioner<IntersectionVehicleRequestKey, SsmStatus>())
                                .withKeySerde(JsonSerdes.IntersectionVehicleRequestKey())
                                .withValueSerde(JsonSerdes.SsmStatus())
                )
                .peek((key, value) -> {
                    log.info("SsmStatus: {}, {}", key, value);
                });



        var eventStream = ssmStatusStream
            .leftJoin(
                srmRequestTable,

                // ValueJoiner
                (ssmStatus, srmRequest) -> {
                    log.info("Joining: {}, {}", ssmStatus, srmRequest);
                    JoinedRequestStatus joined = new JoinedRequestStatus(srmRequest, ssmStatus);
                    log.info("JoinedRequestStatus: {}", joined);
                    return joined;
                    },
                // Join with grace period to handle late and out-of order messages
                // The minimum Kafka Streams version for this join with grace period and version store to play well with
                // TopologyTestDriver is 3.9.1.  The test for this does not pass with 3.7 or 3.8
                Joined.<IntersectionVehicleRequestKey, SsmStatus, SrmRequest>as("joined-request-status")
                        .withKeySerde(JsonSerdes.IntersectionVehicleRequestKey())
                        .withValueSerde(JsonSerdes.SsmStatus())
                        .withOtherValueSerde(JsonSerdes.SrmRequest()))
//                        .withGracePeriod(ssmStreamGracePeriod))
            .peek((key, value) -> {
                log.info("peek JoinedRequestStatus: {}, {}", key, value);
            })
            .filter((key, value) -> {
                log.info("Filtering: {}, {}", key, value);
                // Filter out SSMs that weren't joined with an SRM, but log as a warning
                if (value.getSrmRequest() == null) {
                    if (parameters.isDebug()) {
                        log.warn("No SRM request found to match SSM Status: {}", value.getSsmStatus());
                    }
                    return false;
                }

                // Filter out SSM responses that don't have a final status
                SsmStatus ssmStatus = value.getSsmStatus();

                if (parameters.isDebug()) {
                    log.info("Joined SSM Status with SRM Request: {}", value);
                }

                return true;
            })

            // Produce Event
            .mapValues(value -> {
                var event = new PriorityPreemptionRequestEvent();
                var request = value.getSrmRequest();
                var status = value.getSsmStatus();
                event.setIntersectionID(request.getIntersectionId());
                event.setRoadRegulatorID(request.getRegion());
                event.setVehicleId(request.getVehicleId());
                event.setRequestId(status.getRequestId());
                event.setRequestTimestamp(request.getTimestamp());
                event.setPriorityRequestType(request.getRequestType());
                event.setVehicleType(request.getVehicleType());
                event.setPriorityRequestType(request.getRequestType());
                event.setInboundLaneId(request.getInboundLaneId());
                event.setInboundApproachId(request.getInboundApproachId());
                event.setInboundLaneConnectionId(request.getInboundLaneConnectionId());
                event.setOutboundLaneId(request.getOutboundLaneId());
                event.setOutboundApproachId(request.getOutboundApproachId());
                event.setOutboundLaneConnectionId(request.getOutboundLaneConnectionId());
                event.setTimeOfLastResponse(status.getTimestamp());
                event.setStatus(status.getStatus());
                if (parameters.isDebug()) {
                    log.info("SSM/SRM Event: {}, has final status: {}", event, status.isFinalStatus());
                }
                return event;
            })

            // Filter out non-final status
            .filter((key, value) -> value.isFinalStatus());

//        // Count SSM Responses with granted status for fulfillment metric
//        KStream<IntersectionVehicleTypeKey, PriorityRequestMetrics> metricsStream =
//            priorityRequestMetricsStreamsAlgorithm.buildTopology(builder, eventStream);

        // Write to event topic
        eventStream.to(parameters.getOutputEventTopic(),
            Produced.with(
                    JsonSerdes.IntersectionVehicleRequestKey(),
                    JsonSerdes.PriorityPreemptionRequestEvent(),
                    new IntersectionIdPartitioner<>()));

//        // Write to metrics topic
//        metricsStream.to(priorityRequestMetricsStreamsAlgorithm.getParameters().getOutputMetricTopic(),
//                Produced.with(
//                        JsonSerdes.IntersectionVehicleTypeKey(),
//                        JsonSerdes.PriorityRequestMetrics(),
//                        new IntersectionIdPartitioner<>()));

        return builder.build();
    }

    @Override
    public PriorityRequestMetricsAlgorithm getPriorityRequestMetricsAlgorithm() {
        return priorityRequestMetricsStreamsAlgorithm;
    }

    @Override
    public void setPriorityRequestMetricsAlgorithm(PriorityRequestMetricsAlgorithm priorityRequestMetricsAlgorithm) {
        if (priorityRequestMetricsAlgorithm instanceof PriorityRequestMetricsStreamsAlgorithm streamsAlgorithm) {
            this.priorityRequestMetricsStreamsAlgorithm = streamsAlgorithm;
        } else {
            String errMsg = String.format("%s is not an instance of PriorityRequestMetricsStreamsAlgorithm", priorityRequestMetricsAlgorithm);
            log.error(errMsg);
            throw new RuntimeException(errMsg);
        }
    }

    @Override
    protected void validate() {
        super.validate();
        if (priorityRequestMetricsStreamsAlgorithm == null) {
            throw new IllegalStateException("PriorityRequestMetricsStreamsAlgorithm has not been set");
        }
        if (priorityRequestMetricsStreamsAlgorithm.getParameters() != null) {
            throw new IllegalStateException("PriorityRequestMetricsParameters not set");
        }
        if (priorityRequestMetricsStreamsAlgorithm.getCommonParameters() != null) {
            throw new IllegalStateException("Metrics algorithm Common Parameters not set");
        }
    }
}
