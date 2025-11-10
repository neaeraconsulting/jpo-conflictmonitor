package us.dot.its.jpo.conflictmonitor.monitor.topologies.priority_preemption_request;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.CommonMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request.PriorityRequestMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.PriorityPreemptionRequestEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.IntersectionVehicleTypeKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.PriorityRequestMetrics;
import us.dot.its.jpo.conflictmonitor.monitor.models.priority_preemption_request.IntersectionVehicleRequestKey;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedBasicVehicleRole;
import us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedPrioritizationResponseStatus;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.srm.ProcessedPriorityRequestType;
import us.dot.its.jpo.geojsonconverter.serialization.deserializers.JsonDeserializer;
import us.dot.its.jpo.geojsonconverter.serialization.serializers.JsonSerializer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedPrioritizationResponseStatus.GRANTED;
import static us.dot.its.jpo.geojsonconverter.pojos.common.ProcessedPrioritizationResponseStatus.REJECTED;

@Slf4j
public class PriorityRequestMetricsTopologyTest {

    private static final int interval = 1;
    private static final ChronoUnit intervalUnits = ChronoUnit.MINUTES;
    private static final long gracePeriodMs = 0;
    private static final int checkInterval = 20;
    private static final ChronoUnit checkIntervalUnits = ChronoUnit.SECONDS;
    private static final int retentionTime = 3;
    private static final ChronoUnit retentionTimeUnits = ChronoUnit.MINUTES;

    private static final String inputEventTopicName = "topic.CmPriorityPreemptionRequestEvent";
    private static final String outputMetricsTopicName = "topic.CmPriorityRequestMetrics";

    private static final String vehicleId = "ABCD0102";
    private static final int intersectionId = 12115;
    private static final int roadRegulatorId = 22100;
    private static final int requestSequenceNumber = 1;
    private static final int inboundLaneId = 15;
    private static final int outboundLaneId = 22;
    private static final ProcessedBasicVehicleRole vehicleType = ProcessedBasicVehicleRole.PUBLICTRANSPORT;
    private static final ProcessedPriorityRequestType requestType = ProcessedPriorityRequestType.PRIORITYREQUEST;

    @Test
    public void testPriorityRequestMetrics() {
        Topology topology = createTopology();
        // Make sure to start on a time window boundary
        final Instant startWallClock =
                ZonedDateTime.of(2025, 1, 1, 0,0,0, 0, ZoneOffset.UTC)
                        .toInstant();

        final long startTimestamp = startWallClock.toEpochMilli();
        try (TopologyTestDriver driver = new TopologyTestDriver(topology, startWallClock)) {

            var inputEventTopic = driver.createInputTopic(inputEventTopicName,
                    new JsonSerializer<IntersectionVehicleRequestKey>(),
                    new JsonSerializer<PriorityPreemptionRequestEvent>());

            var outputMetricsTopic = driver.createOutputTopic(outputMetricsTopicName,
                    new JsonDeserializer<>(IntersectionVehicleTypeKey.class),
                    new JsonDeserializer<>(PriorityRequestMetrics.class));


            // Send 50/50 ratio granted/rejected events for different request IDs
            final int step = 500;
            final Duration stepDuration = Duration.ofMillis(500);
            final int times = 10;
            for (int offset = 0, requestId = 10; offset < 2*times*step; offset += 2*step, requestId += 2) {
                // Send rejected event
                final long rejectedTimestamp = startTimestamp + offset;
                final var rejectedEventKey = getEventKey(requestId);
                final var rejectedEvent = getEvent(rejectedTimestamp, REJECTED, requestId);
                inputEventTopic.pipeInput(rejectedEventKey, rejectedEvent, rejectedTimestamp);
                driver.advanceWallClockTime(stepDuration);

                // Send granted event
                final long grantedTimestamp = startTimestamp + offset + step;
                final int grantedRequestId = requestId + 1;
                final var grantedEventKey = getEventKey(grantedRequestId);
                final var grantedEvent = getEvent(grantedTimestamp, GRANTED, grantedRequestId);
                inputEventTopic.pipeInput(grantedEventKey, grantedEvent, grantedTimestamp);
                driver.advanceWallClockTime(stepDuration);
            }

            // Advance clock time at least one full interval in steps of checkInterval to get close aggregation window
            Duration intervalDuration = Duration.of(interval, intervalUnits);
            Duration checkIntervalDuration = Duration.of(checkInterval, checkIntervalUnits);
            Duration advancedDuration = Duration.ofMillis(0L);
            while (advancedDuration.compareTo(intervalDuration) <= 0) {
                driver.advanceWallClockTime(checkIntervalDuration);
                advancedDuration = advancedDuration.plus(checkIntervalDuration);
            }

            var resultsList = outputMetricsTopic.readKeyValuesToList();
            for (var result : resultsList) {
                var key = result.key;
                var value = result.value;
                log.info("metric key: {}, value: {}", key, value);
                assertThat(key.getIntersectionId(), equalTo(intersectionId));
                assertThat(key.getRegion(), equalTo(roadRegulatorId));
                assertThat(key.getVehicleType(), equalTo(vehicleType));
                assertThat(value.getFulfillmentRate(), allOf(notNullValue(), equalTo(0.5d)));
                assertThat(value.getNumberOfDistinctSrmRequests(), equalTo((long)2*times));
                assertThat(value.getNumberOfGrantedSsmResponses(), equalTo((long)times));
                assertThat(value.getKey().getVehicleType(),  equalTo(vehicleType));
                assertThat(value.getName(), equalTo("PriorityRequest"));
                assertThat(value.getTimePeriod(), notNullValue());
                assertThat(value.getTimePeriod().getBeginTimestamp(), equalTo(startWallClock.toEpochMilli()));
                assertThat(value.getTimePeriod().getEndTimestamp(), equalTo(startWallClock.plus(intervalDuration).toEpochMilli()));
            }
            assertThat(resultsList, hasSize(1));
        }
    }

    private Topology createTopology() {
        var parameters = getParameters();
        var commonParameters = getCommonParameters();
        var metricsTopology = new PriorityRequestMetricsTopology();
        metricsTopology.setParameters(parameters);
        metricsTopology.setCommonParameters(commonParameters);

        // Create a skeleton topology for the metrics subtopology to plug into
        StreamsBuilder builder = new StreamsBuilder();
        var eventStream = builder.stream(inputEventTopicName,
                Consumed.with(
                        JsonSerdes.IntersectionVehicleRequestKey(),
                        JsonSerdes.PriorityPreemptionRequestEvent()));
        var metricsStream = metricsTopology.buildTopology(builder, eventStream);
        metricsStream.to(outputMetricsTopicName, Produced.with(
                JsonSerdes.IntersectionVehicleTypeKey(),
                JsonSerdes.PriorityRequestMetrics(),
                new IntersectionIdPartitioner<>()
        ));
        return builder.build();
    }

    private IntersectionVehicleRequestKey getEventKey(final int requestId) {
        final var eventKey = new IntersectionVehicleRequestKey();
        eventKey.setIntersectionId(intersectionId);
        eventKey.setRegion(roadRegulatorId);
        eventKey.setRequestId(requestId);
        eventKey.setVehicleId(vehicleId);
        return eventKey;
    }

    private PriorityPreemptionRequestEvent getEvent(long timestamp, ProcessedPrioritizationResponseStatus status,
                                                    final int requestId) {
        final var event = new PriorityPreemptionRequestEvent();
        event.setEventGeneratedAt(timestamp);
        event.setTimeOfLastResponse(timestamp);
        event.setIntersectionID(intersectionId);
        event.setRoadRegulatorID(roadRegulatorId);
        event.setVehicleId(vehicleId);
        event.setVehicleType(vehicleType);
        event.setRequestId(requestId);
        event.setRequestSequenceNumber(requestSequenceNumber);
        event.setPriorityRequestType(requestType);
        event.setInboundLaneId(inboundLaneId);
        event.setOutboundLaneId(outboundLaneId);
        event.setStatus(status);
        return event;
    }

    private PriorityRequestMetricsParameters getParameters() {
        final var params = new PriorityRequestMetricsParameters();
        params.setDebug(true);
        params.setAlgorithm("defaultPriorityRequestMetricsAlgorithm");
        params.setInputEventTopic(inputEventTopicName);
        params.setOutputMetricTopic(outputMetricsTopicName);
        return params;
    }

    private CommonMetricsParameters getCommonParameters() {
        final var params = new CommonMetricsParameters();
        params.setInterval(interval);
        params.setIntervalUnits(intervalUnits);
        params.setGracePeriodMs(gracePeriodMs);
        params.setCheckInterval(checkInterval);
        params.setCheckIntervalUnits(checkIntervalUnits);
        params.setRetentionTime(retentionTime);
        params.setRetentionTimeUnits(retentionTimeUnits);
        return params;
    }
}


