package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.CommonMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation.DynamicLaneActivationMetricsParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.revocable_enabled_lane_alignment.RevocableLaneStatus;
import us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation.DynamicLaneActivationMetrics;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.serialization.deserializers.JsonDeserializer;
import us.dot.its.jpo.geojsonconverter.serialization.serializers.JsonSerializer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@Slf4j
public class DynamicLaneActivationMetricsTopologyTest {

    private static final int interval = 1;
    private static final ChronoUnit intervalUnits = ChronoUnit.MINUTES;
    private static final long gracePeriodMs = 0;
    private static final int checkInterval = 10;
    private static final ChronoUnit checkIntervalUnits = ChronoUnit.SECONDS;
    private static final int retentionTime = 3;
    private static final ChronoUnit retentionTimeUnits = ChronoUnit.MINUTES;

    private static final String inputEventTopicName = "topic.CmRevocableEnabledLaneStatus";
    private static final String outputMetricsTopicName = "topic.CmDynamicLaneActivationMetrics";

    private static final String rsuId = "172.18.0.1";
    private static final int intersectionId = 12115;
    private static final int roadRegulatorId = 22100;

    @Test
    public void testDynamicLaneActivationMetrics() {
        Topology topology = createTopology();
        final Instant startWallClock =
                ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();

        final long startTimestamp = startWallClock.toEpochMilli();
        try (TopologyTestDriver driver = new TopologyTestDriver(topology, startWallClock)) {
            var inputEventTopic = driver.createInputTopic(
                    inputEventTopicName,
                    new JsonSerializer<RsuIntersectionKey>(),
                    new JsonSerializer<RevocableLaneStatus>());

            var outputMetricsTopic = driver.createOutputTopic(
                    outputMetricsTopicName,
                    new JsonDeserializer<>(RsuIntersectionKey.class),
                    new JsonDeserializer<>(DynamicLaneActivationMetrics.class));

            final var key = new RsuIntersectionKey(rsuId, intersectionId, roadRegulatorId);

            // Initial status: lanes 16 and 17, only 16 enabled
            inputEventTopic.pipeInput(key, createStatus(startTimestamp, Set.of(16, 17), Set.of(16)), startTimestamp);
            driver.advanceWallClockTime(Duration.ofMillis(500));

            // Toggle 17 enabled
            inputEventTopic.pipeInput(key, createStatus(startTimestamp + 1000, Set.of(16, 17), Set.of(16, 17)),
                    startTimestamp + 1000);
            driver.advanceWallClockTime(Duration.ofMillis(500));

            // Toggle 16 disabled
            inputEventTopic.pipeInput(key, createStatus(startTimestamp + 2000, Set.of(16, 17), Set.of(17)),
                    startTimestamp + 2000);

            // Advance time to close the window
            Duration intervalDuration = Duration.of(interval, intervalUnits);
            driver.advanceWallClockTime(intervalDuration.plusSeconds(1));

            List<org.apache.kafka.streams.KeyValue<RsuIntersectionKey, DynamicLaneActivationMetrics>> results =
                    outputMetricsTopic.readKeyValuesToList();

            assertThat(results, hasSize(1));
            var metrics = results.get(0).value;
            assertThat(metrics.getKey().getIntersectionId(), equalTo(intersectionId));
            assertThat(metrics.getTimePeriod(), notNullValue());
            assertThat(metrics.getRevocableEnabledLaneStatusTable(), hasSize(2));

            var lane16Changes = metrics.getRevocableEnabledLaneStatusTable()
                    .getChangesForLaneID(16).getStatusChanges();
            assertThat(lane16Changes, hasSize(2));
            // Lane 16: initially enabled, then disabled at +2000ms
            assertThat(lane16Changes.get(0).timestamp(), equalTo(startTimestamp));
            assertThat(lane16Changes.get(0).enabled(), equalTo(true));
            assertThat(lane16Changes.get(1).timestamp(), equalTo(startTimestamp + 2000));
            assertThat(lane16Changes.get(1).enabled(), equalTo(false));

            var lane17Changes = metrics.getRevocableEnabledLaneStatusTable()
                    .getChangesForLaneID(17).getStatusChanges();
            assertThat(lane17Changes, hasSize(2));
            // Lane 17: initially disabled, then enabled at +1000ms
            assertThat(lane17Changes.get(0).timestamp(), equalTo(startTimestamp));
            assertThat(lane17Changes.get(0).enabled(), equalTo(false));
            assertThat(lane17Changes.get(1).timestamp(), equalTo(startTimestamp + 1000L));
            assertThat(lane17Changes.get(1).enabled(), equalTo(true));
        }
    }

    private Topology createTopology() {
        var parameters = getParameters();
        var commonParameters = getCommonParameters();
        var metricsTopology = new DynamicLaneActivationMetricsTopology();
        metricsTopology.setParameters(parameters);
        metricsTopology.setCommonParameters(commonParameters);

        StreamsBuilder builder = new StreamsBuilder();
        var eventStream = builder.stream(
                inputEventTopicName,
                Consumed.with(
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                        JsonSerdes.RevocableLaneStatus()));

        var metricsStream = metricsTopology.buildTopology(builder, eventStream);
        metricsStream.to(outputMetricsTopicName, Produced.with(
                us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                JsonSerdes.DynamicLaneActivationMetrics(),
                new IntersectionIdPartitioner<>()
        ));
        return builder.build();
    }

    private DynamicLaneActivationMetricsParameters getParameters() {
        final var params = new DynamicLaneActivationMetricsParameters();
        params.setDebug(true);
        params.setAlgorithm("defaultDynamicLaneActivationMetricsAlgorithm");
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

    private RevocableLaneStatus createStatus(long timestamp, Set<Integer> revocableLanes, Set<Integer> enabledLanes) {
        var status = new RevocableLaneStatus();
        status.setSource(rsuId);
        status.setIntersectionID(intersectionId);
        status.setRoadRegulatorID(roadRegulatorId);
        status.setTimestamp(timestamp);
        status.setRevocableLaneList(new HashSet<>(revocableLanes));
        status.setEnabledLaneList(new HashSet<>(enabledLanes));
        return status;
    }
}

