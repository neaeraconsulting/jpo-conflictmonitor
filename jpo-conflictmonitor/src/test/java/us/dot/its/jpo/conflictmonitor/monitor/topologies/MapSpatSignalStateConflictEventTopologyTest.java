package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.revocable_enabled_lane_alignment.MockRevocableEnabledLaneAlignmentStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.map_spat_message_assessment.MapSpatMessageAssessmentParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.SignalStateConflictEvent;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.conflictmonitor.testutils.ResourceUtils;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;

import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Template unit test for MapSpatMessageAssessmentTopology focusing on SignalStateConflictEvent generation.
 *
 * How to use:
 * - Add example JSON fixtures under src/test/resources/us/dot/its/jpo/conflictmonitor/monitor/topologies/
 *   named `SignalStateConflict_ProcessedMap.json` and `SignalStateConflict_ProcessedSpat.json`.
 * - Adjust assertions below to match expected fields from your fixtures.
 */
public class MapSpatSignalStateConflictEventTopologyTest {

    final String kafkaTopicMapInputTopicName = "topic.ProcessedMap";
    final String kafkaTopicSpatInputTopicName = "topic.ProcessedSpat";
    final String kafkaTopicSignalStateConflictEventTopicName = "topic.CmSignalStateConflictEvents";
    final String kafkaTopicSignalGroupAlignmentEventTopicName = "topic.CmSignalGroupAlignmentEvents";
    final String kafkaTopicIntersectionReferenceAlignmentEventTopicName = "topic.CmIntersectionReferenceAlignmentEvents";
    final String kafkaTopicIntersectionReferenceAlignmentNotificationTopicName = "topic.CmIntersectionReferenceAlignmentNotifications";
    final String kafkaTopicSignalGroupAlignmentNotificationTopicName = "topic.CmSignalGroupAlignmentNotifications";
    final String kafkaTopicSignalStateConflictNotificationTopicName = "topic.CmSignalStateConflictNotifications";
    final String kafkaTopicIntersectionReferenceAlignmentNotificationAggTopicName = "topic.CmIntersectionReferenceAlignmentNotificationAggregation";
    final String kafkaTopicSignalGroupAlignmentNotificationAggTopicName = "topic.CmSignalGroupAlignmentNotificationAggregation";
    final String kafkaTopicSignalStateConflictNotificationAggTopicName = "topic.CmSignalStateConflictNotificationAggregation";


    private static final String RESOURCE_PATH = "/us/dot/its/jpo/conflictmonitor/monitor/topologies/";

    @Test
    public void testSignalStateConflictEventGenerated() {

        MapSpatMessageAssessmentTopology mapSpat = new MapSpatMessageAssessmentTopology();
        MapSpatMessageAssessmentParameters parameters = new MapSpatMessageAssessmentParameters();

        // Required topic and behavior parameters
        parameters.setDebug(false);
        parameters.setMapInputTopicName(kafkaTopicMapInputTopicName);
        parameters.setSpatInputTopicName(kafkaTopicSpatInputTopicName);
        parameters.setSignalGroupAlignmentEventTopicName(kafkaTopicSignalGroupAlignmentEventTopicName);
        parameters.setIntersectionReferenceAlignmentEventTopicName(kafkaTopicIntersectionReferenceAlignmentEventTopicName);
        parameters.setSignalStateConflictEventTopicName(kafkaTopicSignalStateConflictEventTopicName);

        // Notification topics
        parameters.setIntersectionReferenceAlignmentNotificationTopicName(kafkaTopicIntersectionReferenceAlignmentNotificationTopicName);
        parameters.setSignalGroupAlignmentNotificationTopicName(kafkaTopicSignalGroupAlignmentNotificationTopicName);
        parameters.setSignalStateConflictNotificationTopicName(kafkaTopicSignalStateConflictNotificationTopicName);

        // Aggregation flags (tests use non-aggregated mode)
        parameters.setAggregateIntersectionReferenceAlignmentEvents(false);
        parameters.setAggregateSignalGroupAlignmentEvents(false);
        parameters.setAggregateSignalStateConflictEvents(false);

        // Aggregated notification topic names (provide defaults)
        parameters.setIntersectionReferenceAlignmentNotificationAggTopicName(kafkaTopicIntersectionReferenceAlignmentNotificationAggTopicName);
        parameters.setSignalGroupAlignmentNotificationAggTopicName(kafkaTopicSignalGroupAlignmentNotificationAggTopicName);
        parameters.setSignalStateConflictNotificationAggTopicName(kafkaTopicSignalStateConflictNotificationAggTopicName);


        mapSpat.setParameters(parameters);

        // The topology references the revocable enabled lane alignment algorithm; inject a mock that does nothing
        mapSpat.setRevocableEnabledLaneAlignmentAlgorithm(new MockRevocableEnabledLaneAlignmentStreamsAlgorithm());

        Topology topology = mapSpat.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {

            TestInputTopic<RsuIntersectionKey, String> inputMapTopic = driver.createInputTopic(
                    kafkaTopicMapInputTopicName,
                    us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey().serializer(),
                    Serdes.String().serializer());

            TestInputTopic<RsuIntersectionKey, String> inputSpatTopic = driver.createInputTopic(
                    kafkaTopicSpatInputTopicName,
                    us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey().serializer(),
                    Serdes.String().serializer());

            TestOutputTopic<RsuIntersectionKey, SignalStateConflictEvent> outputEventTopic = driver.createOutputTopic(
                    kafkaTopicSignalStateConflictEventTopicName,
                    us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey().deserializer(),
                    JsonSerdes.SignalStateConflictEvent().deserializer());

            final String rsuIp = "10.11.81.12";
            final var mapSpatKey = new RsuIntersectionKey(rsuIp, 12109);

            // Load JSON fixtures. Place your sample files at the RESOURCE_PATH location.
            String processedMapJson = ResourceUtils.loadResource(RESOURCE_PATH + "SignalStateConflict_ProcessedMap.json");
            String processedSpatJson = ResourceUtils.loadResource(RESOURCE_PATH + "SignalStateConflict_ProcessedSpat_all_green.json");

            // Pipe JSON strings into the topology (tests in this repo use String payloads for input topics)
            inputMapTopic.pipeInput(mapSpatKey, processedMapJson);
            inputSpatTopic.pipeInput(mapSpatKey, processedSpatJson);

            List<org.apache.kafka.streams.KeyValue<RsuIntersectionKey, SignalStateConflictEvent>> results =
                    outputEventTopic.readKeyValuesToList();

            assertTrue("expected at least one SignalStateConflictEvent", results.size() > 0);
        }
    }

    @Test
    public void testNoSignalStateConflictEventGenerated() {

        MapSpatMessageAssessmentTopology mapSpat = new MapSpatMessageAssessmentTopology();
        MapSpatMessageAssessmentParameters parameters = new MapSpatMessageAssessmentParameters();

        // Required topic and behavior parameters
        parameters.setDebug(false);
        parameters.setMapInputTopicName(kafkaTopicMapInputTopicName);
        parameters.setSpatInputTopicName(kafkaTopicSpatInputTopicName);
        parameters.setSignalGroupAlignmentEventTopicName(kafkaTopicSignalGroupAlignmentEventTopicName);
        parameters.setIntersectionReferenceAlignmentEventTopicName(kafkaTopicIntersectionReferenceAlignmentEventTopicName);
        parameters.setSignalStateConflictEventTopicName(kafkaTopicSignalStateConflictEventTopicName);

        // Notification topics
        parameters.setIntersectionReferenceAlignmentNotificationTopicName(kafkaTopicIntersectionReferenceAlignmentNotificationTopicName);
        parameters.setSignalGroupAlignmentNotificationTopicName(kafkaTopicSignalGroupAlignmentNotificationTopicName);
        parameters.setSignalStateConflictNotificationTopicName(kafkaTopicSignalStateConflictNotificationTopicName);

        // Aggregation flags (tests use non-aggregated mode)
        parameters.setAggregateIntersectionReferenceAlignmentEvents(false);
        parameters.setAggregateSignalGroupAlignmentEvents(false);
        parameters.setAggregateSignalStateConflictEvents(false);

        // Aggregated notification topic names (provide defaults)
        parameters.setIntersectionReferenceAlignmentNotificationAggTopicName(kafkaTopicIntersectionReferenceAlignmentNotificationAggTopicName);
        parameters.setSignalGroupAlignmentNotificationAggTopicName(kafkaTopicSignalGroupAlignmentNotificationAggTopicName);
        parameters.setSignalStateConflictNotificationAggTopicName(kafkaTopicSignalStateConflictNotificationAggTopicName);

        mapSpat.setParameters(parameters);

        // The topology references the revocable enabled lane alignment algorithm; inject a mock that does nothing
        mapSpat.setRevocableEnabledLaneAlignmentAlgorithm(new MockRevocableEnabledLaneAlignmentStreamsAlgorithm());

        Topology topology = mapSpat.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {

            TestInputTopic<RsuIntersectionKey, String> inputMapTopic = driver.createInputTopic(
                    kafkaTopicMapInputTopicName,
                    us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey().serializer(),
                    Serdes.String().serializer());

            TestInputTopic<RsuIntersectionKey, String> inputSpatTopic = driver.createInputTopic(
                    kafkaTopicSpatInputTopicName,
                    us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey().serializer(),
                    Serdes.String().serializer());

            TestOutputTopic<RsuIntersectionKey, SignalStateConflictEvent> outputEventTopic = driver.createOutputTopic(
                    kafkaTopicSignalStateConflictEventTopicName,
                    us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey().deserializer(),
                    JsonSerdes.SignalStateConflictEvent().deserializer());

            final String rsuIp = "10.11.81.12";
            final var mapSpatKey = new RsuIntersectionKey(rsuIp, 12109);

            // Load JSON fixtures. This SPaT should represent an all-green or non-conflicting state.
            String processedMapJson = ResourceUtils.loadResource(RESOURCE_PATH + "SignalStateConflict_ProcessedMap.json");
            String processedSpatJson = ResourceUtils.loadResource(RESOURCE_PATH + "SignalStateConflict_ProcessedSpat.json");

            // Pipe JSON strings into the topology
            inputMapTopic.pipeInput(mapSpatKey, processedMapJson);
            inputSpatTopic.pipeInput(mapSpatKey, processedSpatJson);

            List<org.apache.kafka.streams.KeyValue<RsuIntersectionKey, SignalStateConflictEvent>> results =
                    outputEventTopic.readKeyValuesToList();

            // Expect no conflict events for this SPaT
            assertEquals("expected no SignalStateConflictEvent", 0, results.size());

        }
    }
}
