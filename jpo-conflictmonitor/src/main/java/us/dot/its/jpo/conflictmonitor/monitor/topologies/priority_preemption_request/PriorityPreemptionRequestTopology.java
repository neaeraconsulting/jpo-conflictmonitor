package us.dot.its.jpo.conflictmonitor.monitor.topologies.priority_preemption_request;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
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
import us.dot.its.jpo.conflictmonitor.monitor.processors.priority_preemption_request.PriorityPreemptionRequestTimeoutProcessor;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedBasicVehicleRole;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedSignalRequest;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.SrmProperties;
import us.dot.its.jpo.geojsonconverter.pojos.ssm.ProcessedSignalStatus;

import java.time.Duration;
import java.time.Instant;
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
                        JsonSerdes.IntersectionVehicleRequestSequenceKey(),
                        JsonSerdes.JoinedRequestStatus()
                );
        builder.addStateStore(joinedStoreBuilder);

        final String deduplicateEventsStoreName = parameters.getDeduplicateEventsStoreName();
//        var deduplicateStoreBuilder =
//                Stores.keyValueStoreBuilder(
//                        Stores.persistentKeyValueStore(deduplicateEventsStoreName),
//                        JsonSerdes.IntersectionVehicleRequestSequenceKey(),
//                    Serdes.Long()
//                );
//        builder.addStateStore(deduplicateStoreBuilder);



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

                    List<KeyValue<IntersectionVehicleRequestSequenceKey, SrmRequest>> requestList = new ArrayList<>();

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
                    final long ingestTime = properties.getOdeReceivedAt().toInstant().toEpochMilli();
                    final int requestSequenceNumber = properties.getSequenceNumber();

                    for (final ProcessedSignalRequest processedRequest : requests) {
                        var requestKey = new IntersectionVehicleRequestSequenceKey(vehicleId, processedRequest, requestSequenceNumber);
                        var request = new SrmRequest(vehicleId, vehicleType, timestamp, ingestTime, processedRequest, requestSequenceNumber);
                        requestList.add(new KeyValue<>(requestKey, request));
                    }

                    return requestList;
                })
                .repartition(
                        // Partition by Intersection ID
                        Repartitioned
                                .streamPartitioner(new IntersectionIdPartitioner<IntersectionVehicleRequestSequenceKey, SrmRequest>())
                                .withKeySerde(JsonSerdes.IntersectionVehicleRequestSequenceKey())
                                .withValueSerde(JsonSerdes.SrmRequest())
                );

        if (parameters.isDebug()) {
            srmRequestStream.process(() -> new DiagnosticProcessor<>("SrmRequest Stream", log));
        }

        // Put each SRM request in a KTable to store the latest request with a given
        // intersectionId, region, vehicleId, and requestId
        KTable<IntersectionVehicleRequestSequenceKey, SrmRequest> srmRequestTable =
                srmRequestStream.toTable(
                        // Use versioned state store for the SRM table to be able to deal with out-of-order messages easily
                        // and automatically remove old entries after the retention time
                        Materialized.<IntersectionVehicleRequestSequenceKey, SrmRequest>as(
                                        Stores.persistentVersionedKeyValueStore(
                                                requestStoreName,
                                                retentionTime))
                                .withKeySerde(JsonSerdes.IntersectionVehicleRequestSequenceKey())
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
                    final long ingestTime = processedSsm.getOdeReceivedAt().toInstant().toEpochMilli();
                    List<KeyValue<IntersectionVehicleRequestSequenceKey, SsmStatus>> responseList = new ArrayList<>();
                    List<ProcessedSignalStatus> statusList = processedSsm.getStatusList();
                    for (ProcessedSignalStatus status : statusList) {
                        var key = new IntersectionVehicleRequestSequenceKey(intersectionId, region, status);
                        var ssmStatus = new SsmStatus(intersectionId, region, timestamp, ingestTime, status);
                        responseList.add(new KeyValue<>(key, ssmStatus));
                    }
                    return responseList;
                })
                .repartition(
                        // Partition by Intersection ID
                        Repartitioned.streamPartitioner(new IntersectionIdPartitioner<IntersectionVehicleRequestSequenceKey, SsmStatus>())
                                .withKeySerde(JsonSerdes.IntersectionVehicleRequestSequenceKey())
                                .withValueSerde(JsonSerdes.SsmStatus())
                );

        if (parameters.isDebug()) {
            ssmStatusStream.process(() -> new DiagnosticProcessor<>("SsmStatus Stream", log));
        }

        var ssmStatusTable =
                ssmStatusStream.toTable(
                        Materialized.<IntersectionVehicleRequestSequenceKey, SsmStatus>as(
                                        Stores.persistentVersionedKeyValueStore(
                                                statusStoreName,
                                                retentionTime))
                                .withKeySerde(JsonSerdes.IntersectionVehicleRequestSequenceKey())
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
        KStream<IntersectionVehicleRequestSequenceKey, PriorityPreemptionRequestEvent> eventStream = joinedStream
                .filter((key, value) -> {
                    // Filter null SSMs
                    if (value.getSsmStatus() == null) {
                        return false;
                    }

                    // Filter out SSMs that weren't joined with an SRM, but log as a warning
                    if (value.getSrmRequest() == null) {
                        return false;
                    }

                    // Filter out SSM responses that don't have a final status.
                    // Otherwise, if nothing missing and there is a final status, produce an event
                    return value.getSsmStatus().isFinalStatus();
                })

                // Produce Event
                .mapValues(value -> {
                    var event = value.toEvent();
                    if (parameters.isDebug()) {
                        log.trace("SSM/SRM Event (pre-deduplicated): {}", event);
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



        // Filter out non-final events
        var filteredEventStream = mergedEventStream
                // Filter out non-final statuses only used in metrics
                .filter((key, value)
                        -> value.hasFinalStatus());


        // Read in keys of events that were already sent to a KTable for deduplicating output events
        // Use versioned state store with the same retention time as the SRM and SSM KTables
        // to suppress duplicates during the retention time
        var deduplicateTable = builder
                .stream(parameters.getOutputEventTopic(),
                        Consumed.with(JsonSerdes.IntersectionVehicleRequestSequenceKey(),
                                JsonSerdes.PriorityPreemptionRequestEvent()))

                // Don't need to store the entire event, we only care about the key, convert the value to a timestamp.
                // Use a contextual processor, instead of a simple mapValues function, to be able to get the system
                // time from the processor context.
                .process(() -> new ContextualProcessor<IntersectionVehicleRequestSequenceKey,
                        PriorityPreemptionRequestEvent, IntersectionVehicleRequestSequenceKey, Long>() {
                    @Override
                    public void process(Record<IntersectionVehicleRequestSequenceKey, PriorityPreemptionRequestEvent> record) {
                        final var key = record.key();
                        final var streamTime = record.timestamp();
                        final var wallClockTime = context().currentSystemTimeMs();
                        final var value = record.value();
                        if (value == null) {
                            // Pass tombstones through to clear the ktable store
                            var tombstoneRecord = new Record<>(key, (Long)null, streamTime);
                            if (parameters.isDebug()) {
                                log.debug("Processor passing through tombstone record for key: {}", key);
                            }
                            context().forward(tombstoneRecord);
                        } else {
                            // Value of new record is wall clock timestamp
                            var newRecord = new Record<>(key, wallClockTime, streamTime);
                            context().forward(newRecord);
                        }
                    }
                })
                .toTable(
                    Materialized.<IntersectionVehicleRequestSequenceKey, Long>as(
                                Stores.persistentKeyValueStore(
                                        deduplicateEventsStoreName))
                        .withKeySerde(JsonSerdes.IntersectionVehicleRequestSequenceKey())
                        .withValueSerde(Serdes.Long()
                        )
                );


        var deduplicatedEventStream = filteredEventStream
                // Join with the table of previously sent events
                .leftJoin(deduplicateTable,
                        // Value Joiner.  Written as lambda, not method reference, for clarity
                        (event, timeOfPreviousEvent) -> Pair.of(event, timeOfPreviousEvent),
                        // Join serdes with grace period for versioned store
                        Joined.with(JsonSerdes.IntersectionVehicleRequestSequenceKey(),
                                    JsonSerdes.PriorityPreemptionRequestEvent(),
                                    Serdes.Long())
                                )
                // Filer out events with keys already in the previous table that were sent within the retention time
                // And extract the event.  Use a contextual processor to get the system time from the processor context,
                // instead of using Instant.noew(), to be able to mock the wall clock time in tests.
                .process(() -> new ContextualProcessor<IntersectionVehicleRequestSequenceKey,
                        Pair<PriorityPreemptionRequestEvent, Long>,
                        IntersectionVehicleRequestSequenceKey,
                        PriorityPreemptionRequestEvent>() {

                    // KTable store is value-and-timestamp store
                    KeyValueStore<IntersectionVehicleRequestSequenceKey, ValueAndTimestamp<Long>> dedupStore;

                    @Override
                    public void init(ProcessorContext<IntersectionVehicleRequestSequenceKey, PriorityPreemptionRequestEvent> context) {
                        super.init(context);
                        context().schedule(retentionTime, PunctuationType.WALL_CLOCK_TIME, this::punctuate);
                        dedupStore = context().getStateStore(deduplicateEventsStoreName);
                        if (dedupStore == null) {
                            log.error("Dedup store not found in processor");
                        }
                    }

                    @Override
                    public void process(Record<IntersectionVehicleRequestSequenceKey, Pair<PriorityPreemptionRequestEvent, Long>> record) {
                        Pair<PriorityPreemptionRequestEvent, Long> timestampedEvent = record.value();
                        final Long eventTimestamp = timestampedEvent.getRight();
                        final Instant eventTime = eventTimestamp != null ? Instant.ofEpochMilli(eventTimestamp) : null;
                        final Instant wallClockTime = Instant.ofEpochMilli(context().currentSystemTimeMs());
                        // Forward the record if it doesn't have an entry in the deduplicate table, or if the entry
                        // has a timestamp longer ago than the retention time
                        if (eventTime == null || eventTime.plus(retentionTime).isBefore(wallClockTime)) {
                            var newRecord = new Record<>(record.key(), record.value().getLeft(), record.timestamp());
                            context().forward(newRecord);
                        }
                    }

                    // Punctuator checks if retention time is passed for each key and sends a tombstone to the topic
                    // to clear the deduplicator ktable store
                    private void punctuate(long punctuationTime) {
                        log.info("punctuate");
                        var keysToDelete = new ArrayList<IntersectionVehicleRequestSequenceKey>();
                        try (KeyValueIterator<IntersectionVehicleRequestSequenceKey, ValueAndTimestamp<Long>> iterator = dedupStore.all()) {
                            while (iterator.hasNext()) {
                                KeyValue<IntersectionVehicleRequestSequenceKey, ValueAndTimestamp<Long>> item = iterator.next();
                                final var key = item.key;
                                final var value = item.value;
                                final Long storedTimestamp = value != null ? value.value() : null;
                                if (storedTimestamp == null) continue;
                                final Instant eventTime = Instant.ofEpochMilli(storedTimestamp);
                                final Instant wallClockTime = Instant.ofEpochMilli(context().currentSystemTimeMs());
                                if (eventTime.plus(retentionTime).isBefore(wallClockTime)) {
                                    if (parameters.isDebug()) {
                                        log.debug("punctuator will delete key {}.  eventTime {} plus retentionTime {} is before wallClockTime {}",
                                                key, eventTime, retentionTime, wallClockTime);
                                    }
                                    keysToDelete.add(key);
                                }
                            }
                        }
                        for (IntersectionVehicleRequestSequenceKey key : keysToDelete) {
                            var tombstoneRecord = new Record<>(key, (PriorityPreemptionRequestEvent)null, punctuationTime);
                            context().forward(tombstoneRecord);
                        }
                    }
                },
                deduplicateEventsStoreName
                );

        // Produce deduplicated Event
        if (parameters.isDebug()) {
            deduplicatedEventStream.peek((key, event) -> {
                log.info("SSM/SRM Event (deduplicated): {}", event);
            });
        }


        // Write to event topic
        deduplicatedEventStream.to(parameters.getOutputEventTopic(),
                Produced.with(
                        JsonSerdes.IntersectionVehicleRequestSequenceKey(),
                        JsonSerdes.PriorityPreemptionRequestEvent(),
                        new IntersectionIdPartitioner<>()));

        // Metrics

        // Count SSM Responses with granted status for fulfillment metric
        // Include timeout events without a final status in the metrics
        // Rekey stream for metrics
        var rekeyedEventStream = mergedEventStream
                .selectKey((key, event) -> {
                    var newKey = new IntersectionVehicleRequestKey();
                    newKey.setIntersectionId(key.getIntersectionId());
                    newKey.setRegion(key.getRegion());
                    newKey.setVehicleId(key.getVehicleId());
                    newKey.setRegion(key.getRequestId());
                    return newKey;
                });

        KStream<IntersectionVehicleTypeKey, PriorityRequestMetrics> metricsStream =
                priorityRequestMetricsStreamsAlgorithm.buildTopology(builder, rekeyedEventStream);

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
