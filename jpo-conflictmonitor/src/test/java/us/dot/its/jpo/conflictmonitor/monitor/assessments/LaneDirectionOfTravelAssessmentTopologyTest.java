package us.dot.its.jpo.conflictmonitor.monitor.assessments;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.lane_direction_of_travel_assessment.LaneDirectionOfTravelAssessmentParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.assessments.LaneDirectionOfTravelAssessment;
import us.dot.its.jpo.conflictmonitor.monitor.models.assessments.LaneDirectionOfTravelAssessmentGroup;
import us.dot.its.jpo.conflictmonitor.monitor.models.notifications.LaneDirectionOfTravelNotification;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.assessments.LaneDirectionOfTravelAssessmentTopology;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;


public class LaneDirectionOfTravelAssessmentTopologyTest {
    String kafkaTopicLaneDirectionOfTravelEvent = "topic.CmLaneDirectionOfTravelEvent";
    String kafkaTopicLaneDirectionOfTravelAssessment = "topic.CmLaneDirectionOfTravelAssessment";
    String laneDirectionOfTravelEventKey = "12109";
    String laneDirectionOfTravelEvent = "{\"eventGeneratedAt\":1673394387458,\"eventType\":\"LaneDirectionOfTravel\",\"timestamp\":1655493252811,\"roadRegulatorID\":0,\"intersectionID\":12109,\"laneID\":12,\"laneSegmentNumber\":8,\"laneSegmentInitialLatitude\":39.58972728935065,\"laneSegmentInitialLongitude\":-105.091329041372,\"laneSegmentFinalLatitude\":39.59003379187557,\"laneSegmentFinalLongitude\":-105.09136780827767,\"expectedHeading\":100.25,\"medianVehicleHeading\":174.25,\"medianDistanceFromCenterline\":96.50633375287359,\"aggregateBSMCount\":12}";
    String laneDirectionOfTravelHeadingNotificationEvent = "{\"eventGeneratedAt\":1673394387458,\"eventType\":\"LaneDirectionOfTravel\",\"timestamp\":1655493252811,\"roadRegulatorID\":0,\"intersectionID\":12109,\"laneID\":12,\"laneSegmentNumber\":8,\"laneSegmentInitialLatitude\":39.58972728935065,\"laneSegmentInitialLongitude\":-105.091329041372,\"laneSegmentFinalLatitude\":39.59003379187557,\"laneSegmentFinalLongitude\":-105.09136780827767,\"expectedHeading\":100.25,\"medianVehicleHeading\":174.25,\"medianDistanceFromCenterline\":25,\"aggregateBSMCount\":12}";
    String laneDirectionOfTravelCenterlineNotificationEvent = "{\"eventGeneratedAt\":1673394387458,\"eventType\":\"LaneDirectionOfTravel\",\"timestamp\":1655493252811,\"roadRegulatorID\":0,\"intersectionID\":12109,\"laneID\":12,\"laneSegmentNumber\":8,\"laneSegmentInitialLatitude\":39.58972728935065,\"laneSegmentInitialLongitude\":-105.091329041372,\"laneSegmentFinalLatitude\":39.59003379187557,\"laneSegmentFinalLongitude\":-105.09136780827767,\"expectedHeading\":174.25,\"medianVehicleHeading\":174.25,\"medianDistanceFromCenterline\":96.50633375287359,\"aggregateBSMCount\":12}";
    String laneDirectionOfTravelAssessmentNotificationOutputTopicName = "topic.CmLaneDirectionOfTravelNotification";


    @Test
    public void testAssessment() {
        LaneDirectionOfTravelAssessmentTopology assessment = new LaneDirectionOfTravelAssessmentTopology();
        LaneDirectionOfTravelAssessmentParameters parameters = new LaneDirectionOfTravelAssessmentParameters();
        parameters.setDebug(false);
        parameters.setHeadingToleranceDegrees(20);
        parameters.setHeadingToleranceFirstSegmentDegrees(20);
        parameters.setLaneDirectionOfTravelEventTopicName(kafkaTopicLaneDirectionOfTravelEvent);
        parameters.setLaneDirectionOfTravelAssessmentOutputTopicName(kafkaTopicLaneDirectionOfTravelAssessment);
        parameters.setLookBackPeriodDays(60);
        parameters.setLookBackPeriodGraceTimeSeconds(30);
        parameters.setLaneDirectionOfTravelNotificationOutputTopicName(laneDirectionOfTravelAssessmentNotificationOutputTopicName);
        parameters.setMinimumNumberOfEvents(1);
        parameters.setDistanceFromCenterlineToleranceCm(50);
        assessment.setParameters(parameters);


        Topology topology = assessment.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                kafkaTopicLaneDirectionOfTravelEvent, 
                Serdes.String().serializer(), 
                Serdes.String().serializer());


            TestOutputTopic<String, LaneDirectionOfTravelAssessment> outputTopic = driver.createOutputTopic(
                kafkaTopicLaneDirectionOfTravelAssessment, 
                Serdes.String().deserializer(), 
                JsonSerdes.LaneDirectionOfTravelAssessment().deserializer());
            
            inputTopic.pipeInput(laneDirectionOfTravelEventKey, laneDirectionOfTravelEvent);

            List<KeyValue<String, LaneDirectionOfTravelAssessment>> assessmentResults = outputTopic.readKeyValuesToList();
            
            assertEquals(assessmentResults.size(),1);

            LaneDirectionOfTravelAssessment output = assessmentResults.get(0).value;
            
            assertEquals(output.getRoadRegulatorID(), 0);
            assertEquals(output.getIntersectionID(), 12109);
            
            List<LaneDirectionOfTravelAssessmentGroup> groups = output.getLaneDirectionOfTravelAssessmentGroup();
            assertEquals(groups.size(), 1);
            
            LaneDirectionOfTravelAssessmentGroup group = groups.get(0);
            assertEquals(group.getLaneID(), 12);
            assertEquals(group.getSegmentID(), 8);
            assertEquals(group.getInToleranceEvents(), 0);
            assertEquals(group.getOutOfToleranceEvents(), 1);
            assertEquals(group.getMedianInToleranceHeading(), 0);
            assertEquals(group.getMedianInToleranceCenterlineDistance(), 0);
            assertEquals(group.getMedianHeading(), 174.25);
            assertEquals(group.getMedianCenterlineDistance(), 96.50633375287359);
            assertEquals(group.getTolerance(), 20);

           
        }
    }

    @Test
    public void testNotification() {
        LaneDirectionOfTravelAssessmentTopology assessment = new LaneDirectionOfTravelAssessmentTopology();
        LaneDirectionOfTravelAssessmentParameters parameters = new LaneDirectionOfTravelAssessmentParameters();
        parameters.setDebug(false);
        parameters.setHeadingToleranceDegrees(20);
        parameters.setHeadingToleranceFirstSegmentDegrees(20);
        parameters.setLaneDirectionOfTravelEventTopicName(kafkaTopicLaneDirectionOfTravelEvent);
        parameters.setLaneDirectionOfTravelAssessmentOutputTopicName(kafkaTopicLaneDirectionOfTravelAssessment);
        parameters.setLookBackPeriodDays(60);
        parameters.setLookBackPeriodGraceTimeSeconds(30);
        parameters.setLaneDirectionOfTravelAssessmentOutputTopicName(kafkaTopicLaneDirectionOfTravelAssessment);
        parameters.setLaneDirectionOfTravelNotificationOutputTopicName(laneDirectionOfTravelAssessmentNotificationOutputTopicName);
        parameters.setMinimumNumberOfEvents(1);
        parameters.setDistanceFromCenterlineToleranceCm(50);
        assessment.setParameters(parameters);


        Topology topology = assessment.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                kafkaTopicLaneDirectionOfTravelEvent, 
                Serdes.String().serializer(), 
                Serdes.String().serializer());

            TestOutputTopic<String, LaneDirectionOfTravelNotification> notificationOutputTopic = driver.createOutputTopic(
                laneDirectionOfTravelAssessmentNotificationOutputTopicName, 
                Serdes.String().deserializer(), 
                JsonSerdes.LaneDirectionOfTravelAssessmentNotification().deserializer());
            
            inputTopic.pipeInput(laneDirectionOfTravelEventKey, laneDirectionOfTravelHeadingNotificationEvent);

            List<KeyValue<String, LaneDirectionOfTravelNotification>> notificationResults = notificationOutputTopic.readKeyValuesToList();
            
            assertEquals(notificationResults.size(),1);

            LaneDirectionOfTravelNotification output = notificationResults.get(0).value;
            
            assertEquals(output.getNotificationHeading(), "Lane Direction of Travel Assessment");
            assertEquals(output.getNotificationText(), "Lane Direction of Travel Assessment Notification. The median heading: 174 degrees for segment 8 of lane 12 is not within the allowed tolerance 20.0 degrees of the expected heading 100 degrees.");
            assertEquals(output.getNotificationType(), "LaneDirectionOfTravelAssessmentNotification");

            LaneDirectionOfTravelAssessment outputAssessment = output.getAssessment();
            
            assertEquals(outputAssessment.getRoadRegulatorID(), 0);
            assertEquals(outputAssessment.getIntersectionID(), 12109);
            
            List<LaneDirectionOfTravelAssessmentGroup> groups = outputAssessment.getLaneDirectionOfTravelAssessmentGroup();
            assertEquals(groups.size(), 1);
            
            LaneDirectionOfTravelAssessmentGroup group = groups.get(0);
            assertEquals(group.getLaneID(), 12);
            assertEquals(group.getSegmentID(), 8);
            assertEquals(group.getInToleranceEvents(), 0);
            assertEquals(group.getOutOfToleranceEvents(), 1);
            assertEquals(group.getMedianInToleranceHeading(), 0);
            assertEquals(group.getMedianInToleranceCenterlineDistance(), 0);
            assertEquals(group.getMedianHeading(), 174.25);
            assertEquals(group.getMedianCenterlineDistance(), 25);
            assertEquals(group.getTolerance(), 20);

        }
    }

    @Test
    public void testCenterlineDistanceNotification() {
        LaneDirectionOfTravelAssessmentTopology assessment = new LaneDirectionOfTravelAssessmentTopology();
        LaneDirectionOfTravelAssessmentParameters parameters = new LaneDirectionOfTravelAssessmentParameters();
        parameters.setDebug(false);
        parameters.setHeadingToleranceDegrees(20);
        parameters.setHeadingToleranceFirstSegmentDegrees(20);
        parameters.setLaneDirectionOfTravelEventTopicName(kafkaTopicLaneDirectionOfTravelEvent);
        parameters.setLaneDirectionOfTravelAssessmentOutputTopicName(kafkaTopicLaneDirectionOfTravelAssessment);
        parameters.setLookBackPeriodDays(60);
        parameters.setLookBackPeriodGraceTimeSeconds(30);
        parameters.setLaneDirectionOfTravelAssessmentOutputTopicName(kafkaTopicLaneDirectionOfTravelAssessment);
        parameters.setLaneDirectionOfTravelNotificationOutputTopicName(laneDirectionOfTravelAssessmentNotificationOutputTopicName);
        parameters.setMinimumNumberOfEvents(1);
        parameters.setDistanceFromCenterlineToleranceCm(50);
        assessment.setParameters(parameters);


        Topology topology = assessment.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology)) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                kafkaTopicLaneDirectionOfTravelEvent, 
                Serdes.String().serializer(), 
                Serdes.String().serializer());

            TestOutputTopic<String, LaneDirectionOfTravelNotification> notificationOutputTopic = driver.createOutputTopic(
                laneDirectionOfTravelAssessmentNotificationOutputTopicName, 
                Serdes.String().deserializer(), 
                JsonSerdes.LaneDirectionOfTravelAssessmentNotification().deserializer());
            
            inputTopic.pipeInput(laneDirectionOfTravelEventKey, laneDirectionOfTravelCenterlineNotificationEvent);

            List<KeyValue<String, LaneDirectionOfTravelNotification>> notificationResults = notificationOutputTopic.readKeyValuesToList();
            
            assertEquals(notificationResults.size(),1);

            LaneDirectionOfTravelNotification output = notificationResults.get(0).value;
            
            assertEquals(output.getNotificationHeading(), "Lane Direction of Travel Assessment");
            assertEquals(output.getNotificationText(), "Lane Direction of Travel Assessment Notification. The median distance from centerline: 97 cm for segment 8 of lane 12 is not within the allowed tolerance 50.0 cm of the center of the lane.");
            assertEquals(output.getNotificationType(), "LaneDirectionOfTravelAssessmentNotification");

            LaneDirectionOfTravelAssessment outputAssessment = output.getAssessment();
            
            assertEquals(outputAssessment.getRoadRegulatorID(), 0);
            assertEquals(outputAssessment.getIntersectionID(), 12109);
            
            List<LaneDirectionOfTravelAssessmentGroup> groups = outputAssessment.getLaneDirectionOfTravelAssessmentGroup();
            assertEquals(groups.size(), 1);
            
            LaneDirectionOfTravelAssessmentGroup group = groups.get(0);
            assertEquals(group.getLaneID(), 12);
            assertEquals(group.getSegmentID(), 8);
            assertEquals(group.getInToleranceEvents(), 1);
            assertEquals(group.getOutOfToleranceEvents(), 0);
            assertEquals(group.getMedianInToleranceHeading(), 174.25);
            assertEquals(group.getMedianInToleranceCenterlineDistance(), 96.50633375287359);
            assertEquals(group.getMedianHeading(), 174.25);
            assertEquals(group.getMedianCenterlineDistance(), 96.50633375287359);
            assertEquals(group.getTolerance(), 20);

        }
    }

    // Regression test for fix for median calculation near 0/360, which formerly produced false
    // positive events with incorrect 180 degree heading offsets.
    @Test
    public void testWraparoundHeadingDoesNotProduceFalsePositive() {
        LaneDirectionOfTravelAssessmentTopology assessment = new LaneDirectionOfTravelAssessmentTopology();
        LaneDirectionOfTravelAssessmentParameters parameters = new LaneDirectionOfTravelAssessmentParameters();
        parameters.setDebug(false);
        parameters.setHeadingToleranceDegrees(20);
        parameters.setHeadingToleranceFirstSegmentDegrees(20);
        parameters.setLaneDirectionOfTravelEventTopicName(kafkaTopicLaneDirectionOfTravelEvent);
        parameters.setLaneDirectionOfTravelAssessmentOutputTopicName(kafkaTopicLaneDirectionOfTravelAssessment);
        parameters.setLookBackPeriodDays(60);
        parameters.setLookBackPeriodGraceTimeSeconds(30);
        parameters.setLaneDirectionOfTravelNotificationOutputTopicName(laneDirectionOfTravelAssessmentNotificationOutputTopicName);
        parameters.setMinimumNumberOfEvents(1);
        parameters.setDistanceFromCenterlineToleranceCm(50);
        assessment.setParameters(parameters);

        Topology topology = assessment.buildTopology();

        double[] wraparoundHeadings = {355.0, 358.0, 2.0, 5.0, 359.0, 1.0};

        try (TopologyTestDriver driver = new TopologyTestDriver(topology);
             Serde<String> stringSerde = Serdes.String();
             Serde<LaneDirectionOfTravelAssessment> assessmentSerde = JsonSerdes.LaneDirectionOfTravelAssessment()) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                kafkaTopicLaneDirectionOfTravelEvent,
                stringSerde.serializer(),
                stringSerde.serializer());

            TestOutputTopic<String, LaneDirectionOfTravelAssessment> outputTopic = driver.createOutputTopic(
                kafkaTopicLaneDirectionOfTravelAssessment,
                stringSerde.deserializer(),
                assessmentSerde.deserializer());

            for (double heading : wraparoundHeadings) {
                String event = """
                    {
                      "eventGeneratedAt": 1673394387458,
                      "eventType": "LaneDirectionOfTravel",
                      "timestamp": 1655493252811,
                      "roadRegulatorID": 0,
                      "intersectionID": 12109,
                      "laneID": 12,
                      "laneSegmentNumber": 8,
                      "laneSegmentInitialLatitude": 39.58972728935065,
                      "laneSegmentInitialLongitude": -105.091329041372,
                      "laneSegmentFinalLatitude": 39.59003379187557,
                      "laneSegmentFinalLongitude": -105.09136780827767,
                      "expectedHeading": 2.0,
                      "medianVehicleHeading": %s,
                      "medianDistanceFromCenterline": 0,
                      "aggregateBSMCount": 1
                    }
                    """.formatted(heading);
                inputTopic.pipeInput(laneDirectionOfTravelEventKey, event);
            }

            List<KeyValue<String, LaneDirectionOfTravelAssessment>> assessmentResults = outputTopic.readKeyValuesToList();
            LaneDirectionOfTravelAssessment output = assessmentResults.getLast().value;

            List<LaneDirectionOfTravelAssessmentGroup> groups = output.getLaneDirectionOfTravelAssessmentGroup();
            assertThat(groups.size(), equalTo(1));

            LaneDirectionOfTravelAssessmentGroup group = groups.getFirst();
            assertThat(group.getLaneID(), equalTo(12));
            assertThat(group.getSegmentID(), equalTo(8));
            assertThat(group.getMedianHeading(), closeTo(0.0, 0.01));
            assertThat(group.getInToleranceEvents(), equalTo(6));
            assertThat(group.getOutOfToleranceEvents(), equalTo(0));
            assertThat(LaneDirectionOfTravelAssessmentTopology.headingViolation(group), equalTo(false));
        }
    }

    @Test
    public void testFirstSegmentToleranceAllowsWiderHeadingDeviation() {
        LaneDirectionOfTravelAssessmentTopology assessment = new LaneDirectionOfTravelAssessmentTopology();
        LaneDirectionOfTravelAssessmentParameters parameters = new LaneDirectionOfTravelAssessmentParameters();
        parameters.setDebug(false);
        parameters.setHeadingToleranceDegrees(20);
        parameters.setHeadingToleranceFirstSegmentDegrees(45);
        parameters.setLaneDirectionOfTravelEventTopicName(kafkaTopicLaneDirectionOfTravelEvent);
        parameters.setLaneDirectionOfTravelAssessmentOutputTopicName(kafkaTopicLaneDirectionOfTravelAssessment);
        parameters.setLookBackPeriodDays(60);
        parameters.setLookBackPeriodGraceTimeSeconds(30);
        parameters.setLaneDirectionOfTravelNotificationOutputTopicName(laneDirectionOfTravelAssessmentNotificationOutputTopicName);
        parameters.setMinimumNumberOfEvents(1);
        parameters.setDistanceFromCenterlineToleranceCm(50);
        assessment.setParameters(parameters);

        Topology topology = assessment.buildTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology);
             Serde<String> stringSerde = Serdes.String();
             Serde<LaneDirectionOfTravelAssessment> assessmentSerde = JsonSerdes.LaneDirectionOfTravelAssessment();
             Serde<LaneDirectionOfTravelNotification> notificationSerde = JsonSerdes.LaneDirectionOfTravelAssessmentNotification()) {
            TestInputTopic<String, String> inputTopic = driver.createInputTopic(
                kafkaTopicLaneDirectionOfTravelEvent,
                stringSerde.serializer(),
                stringSerde.serializer());

            TestOutputTopic<String, LaneDirectionOfTravelAssessment> outputTopic = driver.createOutputTopic(
                kafkaTopicLaneDirectionOfTravelAssessment,
                stringSerde.deserializer(),
                assessmentSerde.deserializer());

            TestOutputTopic<String, LaneDirectionOfTravelNotification> notificationOutputTopic = driver.createOutputTopic(
                laneDirectionOfTravelAssessmentNotificationOutputTopicName,
                stringSerde.deserializer(),
                notificationSerde.deserializer());

            String firstSegmentEvent = """
                {
                  "eventGeneratedAt": 1673394387458,
                  "eventType": "LaneDirectionOfTravel",
                  "timestamp": 1655493252811,
                  "roadRegulatorID": 0,
                  "intersectionID": 12109,
                  "laneID": 12,
                  "laneSegmentNumber": 1,
                  "laneSegmentInitialLatitude": 39.58972728935065,
                  "laneSegmentInitialLongitude": -105.091329041372,
                  "laneSegmentFinalLatitude": 39.59003379187557,
                  "laneSegmentFinalLongitude": -105.09136780827767,
                  "expectedHeading": 100.25,
                  "medianVehicleHeading": 130.25,
                  "medianDistanceFromCenterline": 0,
                  "aggregateBSMCount": 1
                }
                """;
            String otherSegmentEvent = """
                {
                  "eventGeneratedAt": 1673394387458,
                  "eventType": "LaneDirectionOfTravel",
                  "timestamp": 1655493252811,
                  "roadRegulatorID": 0,
                  "intersectionID": 12109,
                  "laneID": 12,
                  "laneSegmentNumber": 8,
                  "laneSegmentInitialLatitude": 39.58972728935065,
                  "laneSegmentInitialLongitude": -105.091329041372,
                  "laneSegmentFinalLatitude": 39.59003379187557,
                  "laneSegmentFinalLongitude": -105.09136780827767,
                  "expectedHeading": 100.25,
                  "medianVehicleHeading": 130.25,
                  "medianDistanceFromCenterline": 0,
                  "aggregateBSMCount": 1
                }
                """;
            String firstSegmentBeyondFirstSegmentToleranceEvent = """
                {
                  "eventGeneratedAt": 1673394387458,
                  "eventType": "LaneDirectionOfTravel",
                  "timestamp": 1655493252811,
                  "roadRegulatorID": 0,
                  "intersectionID": 12109,
                  "laneID": 13,
                  "laneSegmentNumber": 1,
                  "laneSegmentInitialLatitude": 39.58972728935065,
                  "laneSegmentInitialLongitude": -105.091329041372,
                  "laneSegmentFinalLatitude": 39.59003379187557,
                  "laneSegmentFinalLongitude": -105.09136780827767,
                  "expectedHeading": 100.25,
                  "medianVehicleHeading": 150.25,
                  "medianDistanceFromCenterline": 0,
                  "aggregateBSMCount": 1
                }
                """;

            inputTopic.pipeInput(laneDirectionOfTravelEventKey, firstSegmentEvent);
            inputTopic.pipeInput(laneDirectionOfTravelEventKey, otherSegmentEvent);
            inputTopic.pipeInput(laneDirectionOfTravelEventKey, firstSegmentBeyondFirstSegmentToleranceEvent);

            List<KeyValue<String, LaneDirectionOfTravelAssessment>> assessmentResults = outputTopic.readKeyValuesToList();
            LaneDirectionOfTravelAssessment output = assessmentResults.getLast().value;

            List<LaneDirectionOfTravelAssessmentGroup> groups = output.getLaneDirectionOfTravelAssessmentGroup();
            assertThat(groups.size(), equalTo(3));

            LaneDirectionOfTravelAssessmentGroup firstSegmentGroup = groups.stream()
                .filter(group -> group.getLaneID() == 12 && group.getSegmentID() == 1)
                .findFirst()
                .orElseThrow();
            assertThat(firstSegmentGroup.getTolerance(), closeTo(45.0, 0.01));
            assertThat(firstSegmentGroup.getInToleranceEvents(), equalTo(1));
            assertThat(firstSegmentGroup.getOutOfToleranceEvents(), equalTo(0));
            assertThat(LaneDirectionOfTravelAssessmentTopology.headingViolation(firstSegmentGroup), equalTo(false));

            LaneDirectionOfTravelAssessmentGroup otherSegmentGroup = groups.stream()
                .filter(group -> group.getLaneID() == 12 && group.getSegmentID() == 8)
                .findFirst()
                .orElseThrow();
            assertThat(otherSegmentGroup.getTolerance(), closeTo(20.0, 0.01));
            assertThat(otherSegmentGroup.getInToleranceEvents(), equalTo(0));
            assertThat(otherSegmentGroup.getOutOfToleranceEvents(), equalTo(1));
            assertThat(LaneDirectionOfTravelAssessmentTopology.headingViolation(otherSegmentGroup), equalTo(true));

            LaneDirectionOfTravelAssessmentGroup firstSegmentBeyondToleranceGroup = groups.stream()
                .filter(group -> group.getLaneID() == 13 && group.getSegmentID() == 1)
                .findFirst()
                .orElseThrow();
            assertThat(firstSegmentBeyondToleranceGroup.getTolerance(), closeTo(45.0, 0.01));
            assertThat(firstSegmentBeyondToleranceGroup.getInToleranceEvents(), equalTo(0));
            assertThat(firstSegmentBeyondToleranceGroup.getOutOfToleranceEvents(), equalTo(1));
            assertThat(LaneDirectionOfTravelAssessmentTopology.headingViolation(firstSegmentBeyondToleranceGroup), equalTo(true));

            List<KeyValue<String, LaneDirectionOfTravelNotification>> notificationResults = notificationOutputTopic.readKeyValuesToList();
            assertThat(notificationResults.size(), equalTo(2));
            assertThat(notificationResults.get(0).value.getNotificationText(), equalTo(
                "Lane Direction of Travel Assessment Notification. The median heading: 130 degrees for segment 8 of lane 12 is not within the allowed tolerance 20.0 degrees of the expected heading 100 degrees."));
            assertThat(notificationResults.get(1).value.getNotificationText(), equalTo(
                "Lane Direction of Travel Assessment Notification. The median heading: 150 degrees for segment 1 of lane 13 is not within the allowed tolerance 45.0 degrees of the expected heading 100 degrees."));
        }
    }
}