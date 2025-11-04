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

    private static final String inputEventTopicName = "topic.CmPriorityPreemptionRequestEvent";
    private static final String outputMetricsTopicName = "topic.CmPriorityRequestMetrics";

    private static final String vehicleId = "ABCD0102";
    private static final int intersectionId = 12115;
    private static final int roadRegulatorId = 22100;
    private static final int requestId = 10;
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
            final var eventKey = getEventKey();

            // Send 50/50 ratio granted/rejected events
            final int step = 500;
            final Duration stepDuration = Duration.ofMillis(500);
            final int times = 10;
            for (int offset = 0; offset < 2*times*step; offset += 2*step) {
                final long rejectedTimestamp = startTimestamp + offset;
                final var rejectedEvent = getEvent(rejectedTimestamp, REJECTED);
                inputEventTopic.pipeInput(eventKey, rejectedEvent, rejectedTimestamp);
                driver.advanceWallClockTime(stepDuration);
                final long grantedTimestamp = startTimestamp + offset + step;
                final var grantedEvent = getEvent(grantedTimestamp, GRANTED);
                inputEventTopic.pipeInput(eventKey, grantedEvent, grantedTimestamp);
                driver.advanceWallClockTime(stepDuration);
            }
            Duration intervalDuration = Duration.of(interval, intervalUnits);

            // TODO remove
//            final long rejectedTimestamp = startTimestamp + intervalDuration.toMillis();
//            final var rejectedEvent = getEvent(rejectedTimestamp, REJECTED);
//            inputEventTopic.pipeInput(eventKey, rejectedEvent, rejectedTimestamp);

            driver.advanceWallClockTime(intervalDuration);

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
        parameters.setDebug(true);
        parameters.setInputEventTopic(inputEventTopicName);
        parameters.setOutputMetricTopic(outputMetricsTopicName);
        var commonParameters = getCommonParameters();
        commonParameters.setInterval(interval);
        commonParameters.setIntervalUnits(intervalUnits);
        commonParameters.setGracePeriodMs(gracePeriodMs);
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

    private IntersectionVehicleRequestKey getEventKey() {
        final var eventKey = new IntersectionVehicleRequestKey();
        eventKey.setIntersectionId(intersectionId);
        eventKey.setRegion(roadRegulatorId);
        eventKey.setRequestId(requestId);
        eventKey.setVehicleId(vehicleId);
        return eventKey;
    }

    private PriorityPreemptionRequestEvent getEvent(long timestamp, ProcessedPrioritizationResponseStatus status) {
        final var event = new PriorityPreemptionRequestEvent();
        event.setEventGeneratedAt(timestamp);
        event.setTimeOfLastResponse(timestamp);
        event.setIntersectionID(intersectionId);
        event.setRoadRegulatorID(roadRegulatorId);
        event.setVehicleId(vehicleId);
        event.setVehicleType(vehicleType);
        event.setRequestId(requestId);
        event.setPriorityRequestType(requestType);
        event.setInboundLaneId(inboundLaneId);
        event.setOutboundLaneId(outboundLaneId);
        event.setStatus(status);
        return event;
    }

    private PriorityRequestMetricsParameters getParameters() {
        return new PriorityRequestMetricsParameters();
    }

    private CommonMetricsParameters getCommonParameters() {
        return new CommonMetricsParameters();
    }
}


