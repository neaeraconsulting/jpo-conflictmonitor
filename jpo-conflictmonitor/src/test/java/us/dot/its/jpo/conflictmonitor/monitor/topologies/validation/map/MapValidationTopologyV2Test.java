package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.map;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import org.mockito.Mockito;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.timestamp_delta.map.MapTimestampDeltaStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.TimestampType;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.map.MapValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.assessments.broadcast_rate.RsuIntersectionTimestampTypeKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate.MapBroadcastRateEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.MapMinimumDataEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.notifications.broadcast_rate.MapBroadcastRateNotification;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.TimestampBoundedQueue;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.TimestampExtractorForBroadcastRate;
import us.dot.its.jpo.conflictmonitor.testutils.TopologyTestUtils;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.ProcessedValidationMessage;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.LineString;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.MapSharedProperties;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;
import us.dot.its.jpo.geojsonconverter.standards.MapStandard;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@Slf4j
public class MapValidationTopologyV2Test {

    final String inputTopicName = "topic.ProcessedMap";
    final String broadcastRateTopicName = "topic.CmMapBroadcastRateEvents";
    final String broadcastRateNotificationTopicName = "topic.CmMapBroadcastRateNotification";
    final String minimumDataTopicName = "topic.CmMapMinimumDataEvents";

    final int v2LowerBoundPairSeparationMs = 975;
    final int v2UpperBoundPairSeparationMs = 1025;
    final int v2LowerBoundDurationPer10MessagesMs = 9975;
    final int v2UpperBoundDurationPer10MessagesMs = 10025;

    final int v2BufferSizeSeconds = 2;
    final int v2BufferGracePeriodMs = 200;

    final int v2AssessmentWindowDuration = 15;
    final ChronoUnit v2AssessmentWindowDurationUnits = ChronoUnit.SECONDS;
    final int v2AssessmentWindowGracePeriodMs = 500;

    final int v2ConformancePercent = 90;

    final int totalSecondsPastFirstWindow = 14;
    final int totalSecondsPastFirstAssessmentWindow = v2AssessmentWindowDuration + v2BufferSizeSeconds + 1;

    final Instant startTime = Instant.ofEpochMilli(1674356310000L);

    final String rsuId = "127.0.0.1";
    final int intersectionId = 11111;
    final int region = 10;

    final String validationMsg = "Validation Message";

    @Test
    public void testCorrectRate_noPairSeparationViolations() {
        log.info("testCorrectRate_noPairSeparationViolations");
        var instants = instantsWithPeriod(1000, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        assertThat(pairEvents(events), empty());
    }

    @Test
    public void testTooSlowRate_producesPairSeparationViolation() {
        log.info("testTooSlowRate_producesPairSeparationViolation");
        var instants = instantsWithPeriod(2000, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        var violations = pairEvents(events);
        assertThat(violations, hasSize(greaterThan(0)));
        assertThat(violations.get(0).getTimePeriod().periodMillis(), equalTo(2000L));
    }

    @Test
    public void testTooFastRate_producesPairSeparationViolation() {
        log.info("testTooFastRate_producesPairSeparationViolation");
        var instants = instantsWithPeriod(500, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        var violations = pairEvents(events);
        assertThat(violations, hasSize(greaterThan(0)));
        assertThat(violations.get(0).getTimePeriod().periodMillis(), equalTo(500L));
    }

    @Test
    public void testPairSeparationAtBoundaries_noViolation() {
        log.info("testPairSeparationAtBoundaries_noViolation");
        var lowerEvents = pipeAndCollectEvents(instantsWithPeriod(v2LowerBoundPairSeparationMs, totalSecondsPastFirstWindow));
        assertThat("lower boundary", pairEvents(lowerEvents), empty());

        var upperEvents = pipeAndCollectEvents(instantsWithPeriod(v2UpperBoundPairSeparationMs, totalSecondsPastFirstWindow));
        assertThat("upper boundary", pairEvents(upperEvents), empty());
    }

    @Test
    public void testCorrectRate_noDurationViolations() {
        log.info("testCorrectRate_noDurationViolations");
        var instants = instantsWithPeriod(1000, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        assertThat(durationEvents(events), empty());
    }

    @Test
    public void testSlightlyOffRate_durationViolationOnly() {
        log.info("testSlightlyOffRate_durationViolationOnly");
        var instants = instantsWithPeriod(1010, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        assertThat(pairEvents(events), empty());
        var violations = durationEvents(events);
        assertThat(violations, hasSize(greaterThan(0)));
        assertThat(violations.get(0).getTimePeriod().periodMillis(), equalTo(10100L));
    }

    @Test
    public void testEventFieldsPopulatedCorrectly() {
        log.info("testEventFieldsPopulatedCorrectly");
        var instants = instantsWithPeriod(2000, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        var violation = pairEvents(events).get(0);

        assertThat(violation.getIntersectionID(), equalTo(intersectionId));
        assertThat(violation.getRoadRegulatorID(), equalTo(region));
        assertThat(violation.getSource(), equalTo(rsuId));
        assertThat(violation.getTopicName(), equalTo(inputTopicName));
        assertThat(violation.getStandard(), equalTo(MapStandard.CTI4501_V2_DRAFT));
        assertThat(violation.getTimestampType(), equalTo(TimestampType.ODE_RECEIVED_AT));
        assertThat(violation.getNumberOfMessages(), equalTo(2));
        assertThat(violation.getTimePeriod(), notNullValue());
        assertThat(violation.getTimePeriod().periodMillis(), equalTo(2000L));
    }

    @Test
    public void testConformantRate_producesPassNotification() {
        log.info("testConformantRate_producesPassNotification");
        var instants = instantsWithPeriod(1000, totalSecondsPastFirstAssessmentWindow);
        var notifications = pipeAndCollectNotifications(instants);
        assertThat(notifications, hasSize(1));

        var notification = notifications.get(0).value;
        assertThat(notification.isPass(), equalTo(true));
        assertThat(notification.getNotificationHeading(), equalTo("MAP Broadcast Rate Assessment: Pass"));
        assertThat(notification.getNotificationText(), notNullValue());
        assertThat(notification.getIntersectionID(), equalTo(intersectionId));
        assertThat(notification.getRoadRegulatorID(), equalTo(region));

        var assessment = notification.getAssessment();
        assertThat(assessment, notNullValue());
        assertThat(assessment.getTimestampType(), equalTo(TimestampType.ODE_RECEIVED_AT));
        assertThat(assessment.getNumberOfMessages(), equalTo((int) expectedMapCount(instants)));
        assertThat(assessment.getNumberOfPairViolations(), equalTo(0));
        assertThat(assessment.getNumberOfDurationViolations(), equalTo(0));
        assertThat(assessment.getMaxPairSeparationMs(), equalTo(0));
        assertThat(assessment.getSource(), containsString(rsuId));
        assertThat(assessment.getTimePeriod().periodMillis(), equalTo(v2AssessmentWindowDuration * 1000L));
    }

    @Test
    public void testNonConformantRate_producesFailNotification() {
        log.info("testNonConformantRate_producesFailNotification");
        var instants = instantsWithPeriod(1200, totalSecondsPastFirstAssessmentWindow);
        var notifications = pipeAndCollectNotifications(instants);
        assertThat(notifications, hasSize(1));

        var notification = notifications.get(0).value;
        assertThat(notification.isPass(), equalTo(false));
        assertThat(notification.getNotificationHeading(), equalTo("MAP Broadcast Rate Assessment: Fail"));
        assertThat(notification.getIntersectionID(), equalTo(intersectionId));
        assertThat(notification.getRoadRegulatorID(), equalTo(region));

        var assessment = notification.getAssessment();
        assertThat(assessment, notNullValue());
        assertThat(assessment.getTimestampType(), equalTo(TimestampType.ODE_RECEIVED_AT));
        assertThat(assessment.getNumberOfMessages(), equalTo((int) expectedMapCount(instants)));
        assertThat(assessment.getNumberOfPairViolations(), greaterThan(0));
        assertThat(assessment.getNumberOfDurationViolations(), greaterThan(0));
        assertThat(assessment.getPercentPairViolations(), greaterThan((double) (100 - v2ConformancePercent)));
        assertThat(assessment.getPercentDurationViolations(), greaterThan((double) (100 - v2ConformancePercent)));
    }

    @Test
    public void testMinDataEventsStillProduced() {
        log.info("testMinDataEventsStillProduced");
        var streamsConfig = createStreamsConfig();
        Topology topology = createTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig);
             Serde<RsuIntersectionKey> rsuIntersectionKeySerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey();
             Serde<ProcessedMap<LineString>> processedMapSerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedMapGeoJson();
             Serde<MapMinimumDataEvent> mapMinimumDataEventSerde = JsonSerdes.MapMinimumDataEvent()) {

            var inputTopic = driver.createInputTopic(inputTopicName,
                    rsuIntersectionKeySerde.serializer(), processedMapSerde.serializer());
            var minimumDataTopic = driver.createOutputTopic(minimumDataTopicName,
                    rsuIntersectionKeySerde.deserializer(), mapMinimumDataEventSerde.deserializer());

            final RsuIntersectionKey key = new RsuIntersectionKey(rsuId, intersectionId, region);
            var instants = instantsWithPeriod(1000, 5);
            for (var instant : instants) {
                inputTopic.pipeInput(key, createNonConformantMap(instant), instant);
            }

            var minDataList = minimumDataTopic.readKeyValuesToList();
            assertThat(minDataList, hasSize(greaterThan(0)));
            var result = minDataList.get(0).value;
            assertThat(result.getIntersectionID(), equalTo(intersectionId));
            assertThat(result.getMissingDataElements(), hasSize(1));
            assertThat(result.getMissingDataElements().get(0), startsWith(validationMsg));
        }
    }

    @Test
    public void testOutOfOrderArrival_sortedBeforeEvaluation() {
        log.info("testOutOfOrderArrival_sortedBeforeEvaluation");
        var instants = instantsWithPeriod(1000, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(withAdjacentPairsSwapped(instants));
        assertThat(events, empty());
    }

    // --- helpers ---

    private List<KeyValue<RsuIntersectionKey, MapBroadcastRateEvent>> pipeAndCollectEvents(List<Instant> pipeOrder) {
        var streamsConfig = createStreamsConfig();
        Topology topology = createTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig);
             Serde<RsuIntersectionKey> rsuIntersectionKeySerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey();
             Serde<ProcessedMap<LineString>> processedMapSerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedMapGeoJson();
             Serde<MapBroadcastRateEvent> mapBroadcastRateEventSerde = JsonSerdes.MapBroadcastRateEvent()) {

            var inputTopic = driver.createInputTopic(inputTopicName,
                    rsuIntersectionKeySerde.serializer(), processedMapSerde.serializer());
            var broadcastRateTopic = driver.createOutputTopic(broadcastRateTopicName,
                    rsuIntersectionKeySerde.deserializer(), mapBroadcastRateEventSerde.deserializer());

            final RsuIntersectionKey key = new RsuIntersectionKey(rsuId, intersectionId, region);
            for (var instant : pipeOrder) {
                inputTopic.pipeInput(key, createMap(instant), instant);
            }

            return broadcastRateTopic.readKeyValuesToList();
        }
    }

    private List<KeyValue<RsuIntersectionTimestampTypeKey, MapBroadcastRateNotification>> pipeAndCollectNotifications(
            List<Instant> pipeOrder) {
        var streamsConfig = createStreamsConfig();
        Topology topology = createTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig);
             Serde<RsuIntersectionKey> rsuIntersectionKeySerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey();
             Serde<ProcessedMap<LineString>> processedMapSerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedMapGeoJson();
             Serde<RsuIntersectionTimestampTypeKey> notificationKeySerde = JsonSerdes.RsuIntersectionTimestampTypeKey();
             Serde<MapBroadcastRateNotification> notificationSerde = JsonSerdes.MapBroadcastRateNotification()) {

            var inputTopic = driver.createInputTopic(inputTopicName,
                    rsuIntersectionKeySerde.serializer(), processedMapSerde.serializer());
            var notificationTopic = driver.createOutputTopic(broadcastRateNotificationTopicName,
                    notificationKeySerde.deserializer(), notificationSerde.deserializer());

            final RsuIntersectionKey key = new RsuIntersectionKey(rsuId, intersectionId, region);
            for (var instant : pipeOrder) {
                inputTopic.pipeInput(key, createMap(instant), instant);
            }

            return notificationTopic.readKeyValuesToList();
        }
    }

    private static List<MapBroadcastRateEvent> filterByNumberOfMessages(
            List<KeyValue<RsuIntersectionKey, MapBroadcastRateEvent>> events, int numberOfMessages) {
        return events.stream()
                .map(kv -> kv.value)
                .filter(event -> event.getNumberOfMessages() == numberOfMessages)
                .collect(Collectors.toList());
    }

    private static List<MapBroadcastRateEvent> pairEvents(List<KeyValue<RsuIntersectionKey, MapBroadcastRateEvent>> events) {
        return filterByNumberOfMessages(events, 2);
    }

    private static List<MapBroadcastRateEvent> durationEvents(List<KeyValue<RsuIntersectionKey, MapBroadcastRateEvent>> events) {
        return filterByNumberOfMessages(events, TimestampBoundedQueue.MAX_NUM_MESSAGES_FOR_DURATION);
    }

    private List<Instant> instantsWithPeriod(int periodMillis, int totalSeconds) {
        return TopologyTestUtils.getInstants(startTime, periodMillis, totalSeconds);
    }

    private long expectedMapCount(List<Instant> instants) {
        long windowEndMillis = startTime.toEpochMilli() + v2AssessmentWindowDuration * 1000L;
        return instants.stream()
                .filter(i -> i.toEpochMilli() < windowEndMillis)
                .count();
    }

    private List<Instant> withAdjacentPairsSwapped(List<Instant> instants) {
        var result = new ArrayList<>(instants);
        for (int i = 0; i + 1 < result.size(); i += 2) {
            var tmp = result.get(i);
            result.set(i, result.get(i + 1));
            result.set(i + 1, tmp);
        }
        return result;
    }

    private Topology createTopology() {
        var parameters = getParameters();
        var mapValidationTopology = new MapValidationTopologyV2();
        mapValidationTopology.setParameters(parameters);
        var timestampDeltaAlgorithm = Mockito.mock(MapTimestampDeltaStreamsAlgorithm.class);
        mapValidationTopology.setTimestampDeltaAlgorithm(timestampDeltaAlgorithm);
        return mapValidationTopology.buildTopology();
    }

    private Properties createStreamsConfig() {
        var streamsConfig = new Properties();
        streamsConfig.setProperty(
                StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                TimestampExtractorForBroadcastRate.class.getName());
        return streamsConfig;
    }

    private MapValidationParameters getParameters() {
        var parameters = new MapValidationParameters();
        parameters.setInputTopicName(inputTopicName);
        parameters.setBroadcastRateTopicName(broadcastRateTopicName);
        parameters.setBroadcastRateNotificationTopicName(broadcastRateNotificationTopicName);
        parameters.setMinimumDataTopicName(minimumDataTopicName);
        parameters.setRollingPeriodSeconds(10);
        parameters.setAggregateMinimumDataEvents(false);
        parameters.setV2BroadcastRateBufferSizeSeconds(v2BufferSizeSeconds);
        parameters.setV2BroadcastRateBufferGracePeriodMs(v2BufferGracePeriodMs);
        parameters.setV2BroadcastRateLowerBoundPairSeparationMs(v2LowerBoundPairSeparationMs);
        parameters.setV2BroadcastRateUpperBoundPairSeparationMs(v2UpperBoundPairSeparationMs);
        parameters.setV2BroadcastRateLowerBoundDurationPer10MessagesMs(v2LowerBoundDurationPer10MessagesMs);
        parameters.setV2BroadcastRateUpperBoundDurationPer10MessagesMs(v2UpperBoundDurationPer10MessagesMs);
        parameters.setV2BroadcastRateAssessmentWindowDuration(v2AssessmentWindowDuration);
        parameters.setV2BroadcastRateAssessmentWindowDurationUnits(v2AssessmentWindowDurationUnits);
        parameters.setV2BroadcastRateAssessmentWindowGracePeriodMs(v2AssessmentWindowGracePeriodMs);
        parameters.setV2BroadcastRateConformancePercent(v2ConformancePercent);
        return parameters;
    }

    private ProcessedMap<LineString> createMap(Instant odeReceivedAt) {
        var map = new ProcessedMap<LineString>();
        var props = new MapSharedProperties();
        map.setProperties(props);
        props.setOdeReceivedAt(odeReceivedAt.atZone(ZoneOffset.UTC));
        props.setCti4501Conformant(true);
        return map;
    }

    private ProcessedMap<LineString> createNonConformantMap(Instant odeReceivedAt) {
        var map = new ProcessedMap<LineString>();
        var props = new MapSharedProperties();
        map.setProperties(props);
        props.setOdeReceivedAt(odeReceivedAt.atZone(ZoneOffset.UTC));
        props.setCti4501Conformant(false);
        var valMsgList = new ArrayList<ProcessedValidationMessage>();
        var msg = new ProcessedValidationMessage();
        msg.setMessage(validationMsg);
        valMsgList.add(msg);
        props.setValidationMessages(valMsgList);
        return map;
    }
}
