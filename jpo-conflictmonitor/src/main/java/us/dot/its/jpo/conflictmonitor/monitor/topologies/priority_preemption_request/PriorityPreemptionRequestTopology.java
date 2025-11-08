package us.dot.its.jpo.conflictmonitor.monitor.topologies.priority_preemption_request;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
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
import us.dot.its.jpo.conflictmonitor.monitor.processors.DiagnosticProcessor;
import us.dot.its.jpo.conflictmonitor.monitor.processors.PriorityPreemptionRequestTimeoutProcessor;
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
        final String statusStoreName = parameters.getSsmStoreName();
        final Duration retentionTime = Duration.of(
                parameters.getStoreRetentionTime(),
                parameters.getRetentionTimeUnits());
        final Duration maxTimeBetweenSrms = Duration.of(
                parameters.getMaxTimeBetweenSrms(),
                parameters.getMaxTimeBetweenSrmsUnits());

        // State store for joined SRM/SSM processor
        final String joinedStoreName = parameters.getJoinedStoreName();
        var joinedStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(joinedStoreName),
                        JsonSerdes.IntersectionVehicleRequestKey(),
                        JsonSerdes.JoinedRequestStatus()
                );
        builder.addStateStore(joinedStoreBuilder);


        // Read ProcessedSrms
        var processedSrmStream = builder
                .stream(
                        parameters.getProcessedSrmInputTopic(),
                        Consumed.with(
                                        RsuVehicleIdKey(),
                                        ProcessedSrm())
                                .withTimestampExtractor(new ProcessedSrmTimestampExtractor()));

        if (parameters.isDebug()) {
            processedSrmStream.process(() -> new DiagnosticProcessor<>("ProcessedSrm Stream", log));
        }

        // Unwrap SRM Requests
        var srmRequestStream = processedSrmStream
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
                );

        if (parameters.isDebug()) {
            srmRequestStream.process(() -> new DiagnosticProcessor<>("SrmRequest Stream", log));
        }

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

        if (parameters.isDebug()) {
            srmRequestTable.toStream().process(() -> new DiagnosticProcessor<>("SrmRequest Versioned KTable", log));
        }

        // Read ProcessedSsms
        var processedSsmStream = builder
                .stream(parameters.getProcessedSsmInputTopic(),
                        Consumed.with(RsuIntersectionKey(), ProcessedSsm())
                                .withTimestampExtractor(new ProcessedSsmTimestampExtractor()));

        if (parameters.isDebug()) {
            processedSsmStream.process(() -> new DiagnosticProcessor<>("ProcessedSsm Stream", log));
        }

        // Unwrap SSM requests
        var ssmStatusStream = processedSsmStream
                .flatMap((rsuIntersectionKey, processedSsm) -> {
                    if (parameters.isDebug()) {
                        log.info("received SSM: key: {}, value: {}", rsuIntersectionKey, processedSsm);
                    }
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
                );

        if (parameters.isDebug()) {
            ssmStatusStream.process(() -> new DiagnosticProcessor<>("SsmStatus Stream", log));
        }

        var ssmStatusTable =
                ssmStatusStream.toTable(
                        Materialized.<IntersectionVehicleRequestKey, SsmStatus>as(
                                        Stores.persistentVersionedKeyValueStore(
                                                statusStoreName,
                                                retentionTime))
                                .withKeySerde(JsonSerdes.IntersectionVehicleRequestKey())
                                .withValueSerde(JsonSerdes.SsmStatus())
                );

        if (parameters.isDebug()) {
            ssmStatusTable.toStream().process(() -> new DiagnosticProcessor<>("SsmStatus Versioned KTable", log));
        }


        var joinedTable = ssmStatusTable
                .outerJoin(
                        srmRequestTable,

                        // ValueJoiner
                        (ssmStatus, srmRequest) -> new JoinedRequestStatus(srmRequest, ssmStatus)
                );

        var joinedStream = joinedTable.toStream();

        // Diagnostic logging of the joined table
        if (parameters.isDebug()) {
            joinedStream.peek((key, value) -> {
                if (value.getSsmStatus() == null) {
                    log.info("JoinedRequestStatus KTable: SRM received, SSM is null");
                    return;
                }

                if (value.getSrmRequest() == null) {
                    log.warn("JoinedRequestStatus KTable: No SRM request found to match SSM Status");
                    return;
                }

                if (!value.getSsmStatus().isFinalStatus()) {
                    log.info("JoinedRequestStatus KTable: SSM status is not final");
                    return;
                }

                log.info("JoinedRequestStatus KTable: SSM and SRM matched and SSM status is final");

            }).process(() -> new DiagnosticProcessor<>("JoinedRequestStatus KTable", log));
        }

        // Check for events from joined stream
        KStream<IntersectionVehicleRequestKey, PriorityPreemptionRequestEvent> eventStream = joinedStream
                .filter((key, value) -> {

                    // Filter null SSMs
                    if (value.getSsmStatus() == null) {
                        return false;
                    }

                    // Filter out SSMs that weren't joined with an SRM, but log as a warning
                    if (value.getSrmRequest() == null) {
                        return false;
                    }

                    // Filter out SSM responses that don't have a final status
                    if (!value.getSsmStatus().isFinalStatus()) {
                        return false;
                    }

                    // Nothing missing and there is a final status: produce an event
                    return true;
                })

                // Produce Event
                .mapValues(value -> {
                    var event = value.toEvent();
                    if (parameters.isDebug()) {
                        log.info("SSM/SRM Event: {}", event);
                    }
                    return event;
                });

        // Check for SRMs that time out without receiving an SSM or with a final status, merge with the
        // stream of events with final status
        var mergedEventStream = joinedStream
                .process(() -> new PriorityPreemptionRequestTimeoutProcessor(
                                maxTimeBetweenSrms,
                                parameters.isDebug(),
                                joinedStoreName),
                        joinedStoreName)
                .merge(eventStream);

        // Count SSM Responses with granted status for fulfillment metric
        // Include timeout events without a final status in the metrics
        KStream<IntersectionVehicleTypeKey, PriorityRequestMetrics> metricsStream =
                priorityRequestMetricsStreamsAlgorithm.buildTopology(builder, eventStream);

        // Write to event topic
        mergedEventStream.to(parameters.getOutputEventTopic(),
                Produced.with(
                        JsonSerdes.IntersectionVehicleRequestKey(),
                        JsonSerdes.PriorityPreemptionRequestEvent(),
                        new IntersectionIdPartitioner<>()));



        if (parameters.isDebug()) {
            metricsStream.process(() -> new DiagnosticProcessor<>("Priority Request Metrics Stream", log));
        }

        // Write to metrics topic
        metricsStream.to(priorityRequestMetricsStreamsAlgorithm.getParameters().getOutputMetricTopic(),
                Produced.with(
                        JsonSerdes.IntersectionVehicleTypeKey(),
                        JsonSerdes.PriorityRequestMetrics(),
                        new IntersectionIdPartitioner<>()));

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
        if (priorityRequestMetricsStreamsAlgorithm.getParameters() == null) {
            throw new IllegalStateException("PriorityRequestMetricsParameters not set");
        }
        if (priorityRequestMetricsStreamsAlgorithm.getCommonParameters() == null) {
            throw new IllegalStateException("Metrics algorithm Common Parameters not set");
        }
    }
}
