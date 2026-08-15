package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
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

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.timestamp_delta.spat.SpatTimestampDeltaStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.TimestampType;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.spat.SpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate.SpatBroadcastRateEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.notifications.broadcast_rate.SpatBroadcastRateNotification;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.spat.SpatValidationTopologyV2;
import us.dot.its.jpo.conflictmonitor.testutils.TopologyTestUtils;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;
import us.dot.its.jpo.geojsonconverter.standards.SpatStandard;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@Slf4j
public class SpatValidationTopologyV2Test {

    final String inputTopicName = "topic.ProcessedSpat";
    final String broadcastRateTopicName = "topic.CmSpatBroadcastRateEvents";
    final String broadcastRateNotificationTopicName = "topic.CmSpatBroadcastRateNotification";
    final String minimumDataTopicName = "topic.CmSpatMinimumDataEvents";

    // v2 broadcast-rate bounds (production defaults).
    final int v2LowerBoundPairSeparationMs = 75;
    final int v2UpperBoundPairSeparationMs = 125;
    final int v2LowerBoundDurationPer10MessagesMs = 975;
    final int v2UpperBoundDurationPer10MessagesMs = 1025;

    // Sort-buffer window/grace, shortened for fewer messages per test.
    final int v2BufferSizeSeconds = 1;
    final int v2BufferGracePeriodMs = 200;

    // assessment window, shortened for test
    final int v2AssessmentWindowDuration = 5;
    final ChronoUnit v2AssessmentWindowDurationUnits = ChronoUnit.SECONDS;
    final int v2AssessmentWindowGracePeriodMs = 500;

    // Pass/fail thresholds (production defaults).
    final int v2ConformancePercent = 90;
    final int v2MaxOutlierPairSeparationMs = 300;

    // Seconds needed to close the first buffer window, plus margin.
    final int totalSecondsPastFirstWindow = v2BufferSizeSeconds + (v2BufferGracePeriodMs / 1000) + 1;

    // Seconds needed to close the first assessment window, plus a buffer window and margin.
    final int totalSecondsPastFirstAssessmentWindow = v2AssessmentWindowDuration + v2BufferSizeSeconds + 1;

    // Start time on a buffer-window boundary.
    final Instant startTime = Instant.ofEpochMilli(1674356320000L);

    final String rsuId = "127.0.0.1";
    final int intersectionId = 11111;
    final int region = 10;

    @Test
    public void testCorrectRate_noPairSeparationViolations() {
        log.info("testCorrectRate_noPairSeparationViolations");
        var instants = instantsWithPeriod(100, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        assertThat(pairEvents(events), empty());
    }

    @Test
    public void testTooSlowRate_producesPairSeparationViolation() {
        log.info("testTooSlowRate_producesPairSeparationViolation");
        var instants = instantsWithPeriod(200, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        var violations = pairEvents(events);
        assertThat(violations, hasSize(greaterThan(0)));
        assertThat(violations.get(0).getTimePeriod().periodMillis(), equalTo(200L));
    }

    @Test
    public void testTooFastRate_producesPairSeparationViolation() {
        log.info("testTooFastRate_producesPairSeparationViolation");
        var instants = instantsWithPeriod(50, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        var violations = pairEvents(events);
        assertThat(violations, hasSize(greaterThan(0)));
        assertThat(violations.get(0).getTimePeriod().periodMillis(), equalTo(50L));
    }

    @Test
    public void testPairSeparationAtLowerBoundary_noViolation() {
        log.info("testPairSeparationAtLowerBoundary_noViolation");
        var instants = instantsWithPeriod(v2LowerBoundPairSeparationMs, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        assertThat(pairEvents(events), empty());
    }

    @Test
    public void testPairSeparationAtUpperBoundary_noViolation() {
        log.info("testPairSeparationAtUpperBoundary_noViolation");
        var instants = instantsWithPeriod(v2UpperBoundPairSeparationMs, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        assertThat(pairEvents(events), empty());
    }

    @Test
    public void testPairSeparationJustOutsideBoundary_violation() {
        log.info("testPairSeparationJustOutsideBoundary_violation");
        var belowLower = pipeAndCollectEvents(
                instantsWithPeriod(v2LowerBoundPairSeparationMs - 1, totalSecondsPastFirstWindow));
        assertThat("below lower bound", pairEvents(belowLower), hasSize(greaterThan(0)));

        var aboveUpper = pipeAndCollectEvents(
                instantsWithPeriod(v2UpperBoundPairSeparationMs + 1, totalSecondsPastFirstWindow));
        assertThat("above upper bound", pairEvents(aboveUpper), hasSize(greaterThan(0)));
    }

    @Test
    public void testCorrectRate_noDurationViolations() {
        log.info("testCorrectRate_noDurationViolations");
        var instants = instantsWithPeriod(100, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        assertThat(durationEvents(events), empty());
    }

    @Test
    public void testSlightlySlowRate_durationViolationOnly() {
        log.info("testSlightlySlowRate_durationViolationOnly");
        var instants = instantsWithPeriod(103, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        assertThat(pairEvents(events), empty());
        var violations = durationEvents(events);
        assertThat(violations, hasSize(greaterThan(0)));
        assertThat(violations.get(0).getTimePeriod().periodMillis(), equalTo(1030L));
    }

    @Test
    public void testSlightlyFastRate_durationViolationOnly() {
        log.info("testSlightlyFastRate_durationViolationOnly");
        var instants = instantsWithPeriod(97, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        assertThat(pairEvents(events), empty());
        var violations = durationEvents(events);
        assertThat(violations, hasSize(greaterThan(0)));
        assertThat(violations.get(0).getTimePeriod().periodMillis(), equalTo(970L));
    }

    @Test
    public void testDurationAtLowerBoundary_noViolation() {
        log.info("testDurationAtLowerBoundary_noViolation");
        // Ten gaps summing to exactly 975ms, each within the pair-separation bounds.
        int[] gapsMillis = {98, 98, 98, 98, 98, 97, 97, 97, 97, 97};
        var instants = withNominalTail(instantsWithGaps(gapsMillis), totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        assertThat(events, empty());
    }

    @Test
    public void testDurationAtUpperBoundary_noViolation() {
        log.info("testDurationAtUpperBoundary_noViolation");
        // Ten gaps summing to exactly 1025ms, each within the pair-separation bounds.
        int[] gapsMillis = {103, 103, 103, 103, 103, 102, 102, 102, 102, 102};
        var instants = withNominalTail(instantsWithGaps(gapsMillis), totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        assertThat(events, empty());
    }

    @Test
    public void testOutOfOrderArrivalWithinBufferWindow_sortedBeforeEvaluation() {
        log.info("testOutOfOrderArrivalWithinBufferWindow_sortedBeforeEvaluation");
        // Mild reordering: swap adjacent messages, keeping each one's own timestamp.
        var instants = instantsWithPeriod(100, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(withAdjacentPairsSwapped(instants));
        assertThat(events, empty());
    }

    @Test
    public void testTimestampBuffer_sortsOutOfOrderTimestamps() {
        log.info("testTimestampBuffer_sortsOutOfOrderTimestamps");
        // Strong reordering: reverse the whole first window; unsorted, this would show violations.
        var instants = instantsWithPeriod(100, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(withFirstWindowReversed(instants));
        assertThat(events, empty());
    }

    @Test
    public void testMessagesSpanningMultipleBufferWindows_noFalsePositiveAtBoundary() {
        log.info("testMessagesSpanningMultipleBufferWindows_noFalsePositiveAtBoundary");
        int totalSeconds = v2BufferSizeSeconds * 2 + (v2BufferGracePeriodMs / 1000) + 1;
        var instants = instantsWithPeriod(100, totalSeconds);
        var events = pipeAndCollectEvents(instants);
        assertThat(events, empty());
    }

    @Test
    public void testEventFieldsPopulatedCorrectly() {
        log.info("testEventFieldsPopulatedCorrectly");
        var instants = instantsWithPeriod(200, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);
        var violation = pairEvents(events).get(0);

        assertThat(violation.getIntersectionID(), equalTo(intersectionId));
        assertThat(violation.getRoadRegulatorID(), equalTo(region));
        assertThat(violation.getSource(), equalTo(rsuId));
        assertThat(violation.getTopicName(), equalTo(inputTopicName));
        assertThat(violation.getStandard(), equalTo(SpatStandard.CTI4501_V2_DRAFT));
        assertThat(violation.getNumberOfMessages(), equalTo(2));
        assertThat(violation.getTimePeriod(), notNullValue());
        assertThat(violation.getTimePeriod().periodMillis(), equalTo(200L));
    }

    @Test
    public void testPairCheck_singleViolationPerOccurrence() {
        log.info("testPairCheck_singleViolationPerOccurrence");
        // Regression test: each violating pair must be reported once per timestamp type, not
        // duplicated/dropped. Checked per type since the embedded and odeReceivedAt streams both
        // report violations independently (and, in this test, at identical periods).
        var instants = instantsWithPeriod(200, totalSecondsPastFirstWindow);
        var events = pipeAndCollectEvents(instants);

        for (var type : TimestampType.values()) {
            var violations = pairEvents(events, type);
            var distinctPeriods = violations.stream()
                    .map(event -> List.of(event.getTimePeriod().getBeginTimestamp(), event.getTimePeriod().getEndTimestamp()))
                    .distinct()
                    .toList();
            assertThat(type.toString(), distinctPeriods, hasSize(violations.size()));
        }
    }

    @Test
    public void testConformantRate_producesPassNotification() {
        log.info("testConformantRate_producesPassNotification");
        var instants = instantsWithPeriod(100, totalSecondsPastFirstAssessmentWindow);
        var notifications = pipeAndCollectNotifications(instants);

        // One notification per timestamp type: embedded-in-message and odeReceivedAt each
        // produce their own independent assessment/notification.
        assertThat(notifications, hasSize(2));

        for (var type : TimestampType.values()) {
            var notification = notificationOfType(notifications, type);

            assertThat(type.toString(), notification.isPass(), equalTo(true));
            assertThat(type.toString(), notification.getNotificationHeading(), equalTo("SPaT Broadcast Rate Assessment: Pass"));
            assertThat(type.toString(), notification.getNotificationText(), notNullValue());
            assertThat(type.toString(), notification.getIntersectionID(), equalTo(intersectionId));
            assertThat(type.toString(), notification.getRoadRegulatorID(), equalTo(region));

            var assessment = notification.getAssessment();
            assertThat(type.toString(), assessment, notNullValue());
            assertThat(type.toString(), assessment.getTimestampType(), equalTo(type));
            // Regression guard: every spat in the window must be counted via the leftJoin, not just
            // ones with a matching violation event (a KTable-driven count would undercount here).
            assertThat(type.toString(), assessment.getNumberOfSpats(), equalTo((int) expectedSpatCount(instants)));
            assertThat(type.toString(), assessment.getNumberOfPairViolations(), equalTo(0));
            assertThat(type.toString(), assessment.getNumberOfDurationViolations(), equalTo(0));
            assertThat(type.toString(), assessment.getMaxPairSeparationMs(), equalTo(0));
            assertThat(type.toString(), assessment.getSource(), containsString(rsuId));
            assertThat(type.toString(), assessment.getTimePeriod().periodMillis(), equalTo(v2AssessmentWindowDuration * 1000L));
        }
    }

    @Test
    public void testNonConformantRate_producesFailNotification() {
        log.info("testNonConformantRate_producesFailNotification");
        var instants = instantsWithPeriod(200, totalSecondsPastFirstAssessmentWindow);
        var notifications = pipeAndCollectNotifications(instants);

        // One notification per timestamp type: embedded-in-message and odeReceivedAt each
        // produce their own independent assessment/notification.
        assertThat(notifications, hasSize(2));

        for (var type : TimestampType.values()) {
            var notification = notificationOfType(notifications, type);

            assertThat(type.toString(), notification.isPass(), equalTo(false));
            assertThat(type.toString(), notification.getNotificationHeading(), equalTo("SPaT Broadcast Rate Assessment: Fail"));
            assertThat(type.toString(), notification.getIntersectionID(), equalTo(intersectionId));
            assertThat(type.toString(), notification.getRoadRegulatorID(), equalTo(region));

            var assessment = notification.getAssessment();
            assertThat(type.toString(), assessment, notNullValue());
            assertThat(type.toString(), assessment.getTimestampType(), equalTo(type));
            // Every spat is counted exactly once even when it also carries a violation, i.e. the
            // leftJoin's placeholder path and its matched-violation path don't double count.
            assertThat(type.toString(), assessment.getNumberOfSpats(), equalTo((int) expectedSpatCount(instants)));
            assertThat(type.toString(), assessment.getNumberOfPairViolations(), greaterThan(0));
            assertThat(type.toString(), assessment.getNumberOfDurationViolations(), greaterThan(0));
            assertThat(type.toString(), assessment.getPercentPairViolations(), greaterThan((double)(100 - v2ConformancePercent)));
            assertThat(type.toString(), assessment.getPercentDurationViolations(), greaterThan((double)(100 - v2ConformancePercent)));

            // Every violating pair is exactly 200ms, under the 300ms outlier limit, so the failure
            // is attributable to the violation percentages rather than the outlier criterion.
            assertThat(type.toString(), assessment.getMaxPairSeparationMs(), equalTo(200));
            assertThat(type.toString(), assessment.getMaxAllowedPairSeparationMs(), equalTo(v2MaxOutlierPairSeparationMs));
        }
    }

    @Test
    public void testAssessmentPercent_countsBoundarySpanningViolationInDenominator() {
        log.info("testAssessmentPercent_countsBoundarySpanningViolationInDenominator");
        int totalSeconds = v2AssessmentWindowDuration * 2 + v2BufferSizeSeconds + 1;
        var instants = new ArrayList<>(instantsWithPeriod(100, totalSeconds));
        Instant windowBoundary = startTime.plusSeconds(v2AssessmentWindowDuration);
        instants.remove(windowBoundary);
        var notifications = pipeAndCollectNotifications(instants);

        var secondWindowNotification = notifications.stream()
                .map(kv -> kv.value)
                .filter(n -> n.getAssessment().getTimestampType() == TimestampType.EMBEDDED_IN_MESSAGE)
                .filter(n -> n.getAssessment().getTimePeriod().getBeginTimestamp() == windowBoundary.toEpochMilli())
                .findFirst()
                .orElseThrow(() -> new AssertionError("No notification found for the second assessment window"));

        var assessment = secondWindowNotification.getAssessment();
        assertThat(assessment.getNumberOfPairViolations(), equalTo(1));
        assertThat(assessment.getPercentPairViolations(),
                equalTo(100.0 * 1 / assessment.getNumberOfSpats()));
    }

    // --- odeReceivedAt-specific tests ---
    // These verify that the second timestamp-processing pipeline (driven by ProcessedSpat's
    // odeReceivedAt field) evaluates independently from the embedded-message-timestamp pipeline,
    // by giving each pipeline a different, divergent sequence of timestamps for the same spats.

    @Test
    public void testOdeReceivedAtPairViolation_onlyTagsOdeReceivedAtType() {
        log.info("testOdeReceivedAtPairViolation_onlyTagsOdeReceivedAtType");
        // Embedded timestamps conformant (100ms); odeReceivedAt timestamps too slow (200ms).
        var pairs = divergentInstantPairs(100, 200, totalSecondsPastFirstWindow);
        var events = pipeAndCollectDivergentEvents(pairs);

        assertThat(pairEvents(events, TimestampType.EMBEDDED_IN_MESSAGE), empty());
        var violations = pairEvents(events, TimestampType.ODE_RECEIVED_AT);
        assertThat(violations, hasSize(greaterThan(0)));
        assertThat(violations.get(0).getTimePeriod().periodMillis(), equalTo(200L));
    }

    @Test
    public void testEmbeddedPairViolation_onlyTagsEmbeddedType() {
        log.info("testEmbeddedPairViolation_onlyTagsEmbeddedType");
        // Embedded timestamps too slow (200ms); odeReceivedAt timestamps conformant (100ms).
        var pairs = divergentInstantPairs(200, 100, totalSecondsPastFirstWindow);
        var events = pipeAndCollectDivergentEvents(pairs);

        assertThat(pairEvents(events, TimestampType.ODE_RECEIVED_AT), empty());
        var violations = pairEvents(events, TimestampType.EMBEDDED_IN_MESSAGE);
        assertThat(violations, hasSize(greaterThan(0)));
        assertThat(violations.get(0).getTimePeriod().periodMillis(), equalTo(200L));
    }

    @Test
    public void testOdeReceivedAtDurationViolation_onlyTagsOdeReceivedAtType() {
        log.info("testOdeReceivedAtDurationViolation_onlyTagsOdeReceivedAtType");
        // Embedded timestamps conformant (100ms); odeReceivedAt timestamps slightly slow (103ms),
        // which only trips the 10-message duration criterion, not the pair-separation one.
        var pairs = divergentInstantPairs(100, 103, totalSecondsPastFirstWindow);
        var events = pipeAndCollectDivergentEvents(pairs);

        assertThat(pairEvents(events, TimestampType.EMBEDDED_IN_MESSAGE), empty());
        assertThat(pairEvents(events, TimestampType.ODE_RECEIVED_AT), empty());
        assertThat(durationEvents(events, TimestampType.EMBEDDED_IN_MESSAGE), empty());
        assertThat(durationEvents(events, TimestampType.ODE_RECEIVED_AT), hasSize(greaterThan(0)));
    }

    @Test
    public void testOdeReceivedAtEventFieldsPopulatedCorrectly() {
        log.info("testOdeReceivedAtEventFieldsPopulatedCorrectly");
        var pairs = divergentInstantPairs(100, 200, totalSecondsPastFirstWindow);
        var events = pipeAndCollectDivergentEvents(pairs);
        var violation = pairEvents(events, TimestampType.ODE_RECEIVED_AT).get(0);

        assertThat(violation.getIntersectionID(), equalTo(intersectionId));
        assertThat(violation.getRoadRegulatorID(), equalTo(region));
        assertThat(violation.getSource(), equalTo(rsuId));
        assertThat(violation.getTopicName(), equalTo(inputTopicName));
        assertThat(violation.getStandard(), equalTo(SpatStandard.CTI4501_V2_DRAFT));
        assertThat(violation.getTimestampType(), equalTo(TimestampType.ODE_RECEIVED_AT));
        assertThat(violation.getNumberOfMessages(), equalTo(2));
        assertThat(violation.getTimePeriod(), notNullValue());
        assertThat(violation.getTimePeriod().periodMillis(), equalTo(200L));
    }

    @Test
    public void testOdeReceivedAtOutOfOrderArrival_sortedIndependentlyOfEmbeddedOrder() {
        log.info("testOdeReceivedAtOutOfOrderArrival_sortedIndependentlyOfEmbeddedOrder");
        // Embedded timestamps arrive strictly in order (conformant); odeReceivedAt timestamps are
        // locally out of order (adjacent pairs swapped) but conformant once sorted. Verifies the
        // odeReceivedAt buffer sorts on its own values rather than relying on embedded ordering.
        var embeddedInstants = instantsWithPeriod(100, totalSecondsPastFirstWindow);
        var odeReceivedAtInstants = withAdjacentPairsSwapped(embeddedInstants);

        var pairs = new ArrayList<Instant[]>();
        for (int i = 0; i < embeddedInstants.size(); i++) {
            pairs.add(new Instant[] {embeddedInstants.get(i), odeReceivedAtInstants.get(i)});
        }

        var events = pipeAndCollectDivergentEvents(pairs);
        assertThat(events, empty());
    }

    @Test
    public void testOdeReceivedAtMalformedTimestamp_isDroppedWithoutCrashingPipeline() {
        log.info("testOdeReceivedAtMalformedTimestamp_isDroppedWithoutCrashingPipeline");
        // A malformed odeReceivedAt must be logged and dropped rather than thrown, and must not
        // affect the embedded-timestamp pipeline, which doesn't depend on odeReceivedAt at all.
        var streamsConfig = createStreamsConfig();
        Topology topology = createTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig);
             Serde<RsuIntersectionKey> rsuIntersectionKeySerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey();
             Serde<ProcessedSpat> processedSpatSerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSpat();
             Serde<SpatBroadcastRateEvent> spatBroadcastRateEventSerde = JsonSerdes.SpatBroadcastRateEvent()) {

            var inputTopic = driver.createInputTopic(inputTopicName,
                    rsuIntersectionKeySerde.serializer(), processedSpatSerde.serializer());
            var broadcastRateTopic = driver.createOutputTopic(broadcastRateTopicName,
                    rsuIntersectionKeySerde.deserializer(), spatBroadcastRateEventSerde.deserializer());

            final RsuIntersectionKey key = new RsuIntersectionKey(rsuId, intersectionId, region);
            var instants = instantsWithPeriod(100, totalSecondsPastFirstWindow);
            for (int i = 0; i < instants.size(); i++) {
                var instant = instants.get(i);
                var spat = createSpat(instant);
                if (i == instants.size() / 2) {
                    spat.setOdeReceivedAt("not-a-timestamp");
                }
                inputTopic.pipeInput(key, spat, instant);
            }

            var events = broadcastRateTopic.readKeyValuesToList();
            assertThat(pairEvents(events, TimestampType.EMBEDDED_IN_MESSAGE), empty());
        }
    }

    @Test
    public void testOdeReceivedAtAssessment_independentPassFailPerType() {
        log.info("testOdeReceivedAtAssessment_independentPassFailPerType");
        // Embedded timestamps conformant; odeReceivedAt timestamps too slow. The two notification
        // types must diverge independently rather than sharing a pass/fail outcome. (odeReceivedAt's
        // stream time can outpace embedded's and close more than one assessment window, so this
        // checks the earliest window of each type rather than an exact total count.)
        var pairs = divergentInstantPairs(100, 200, totalSecondsPastFirstAssessmentWindow);
        var notifications = pipeAndCollectDivergentNotifications(pairs);

        assertThat(notifications, hasSize(greaterThanOrEqualTo(2)));

        var embeddedNotification = notificationOfType(notifications, TimestampType.EMBEDDED_IN_MESSAGE);
        assertThat(embeddedNotification.isPass(), equalTo(true));
        assertThat(embeddedNotification.getAssessment().getTimestampType(), equalTo(TimestampType.EMBEDDED_IN_MESSAGE));
        assertThat(embeddedNotification.getAssessment().getNumberOfPairViolations(), equalTo(0));

        var odeReceivedAtNotification = notificationOfType(notifications, TimestampType.ODE_RECEIVED_AT);
        assertThat(odeReceivedAtNotification.isPass(), equalTo(false));
        assertThat(odeReceivedAtNotification.getAssessment().getTimestampType(), equalTo(TimestampType.ODE_RECEIVED_AT));
        assertThat(odeReceivedAtNotification.getAssessment().getNumberOfPairViolations(), greaterThan(0));
    }

    // --- helpers ---

    private List<KeyValue<RsuIntersectionKey, SpatBroadcastRateEvent>> pipeAndCollectEvents(List<Instant> pipeOrder) {
        var streamsConfig = createStreamsConfig();
        Topology topology = createTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig);
             Serde<RsuIntersectionKey> rsuIntersectionKeySerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey();
             Serde<ProcessedSpat> processedSpatSerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSpat();
             Serde<SpatBroadcastRateEvent> spatBroadcastRateEventSerde = JsonSerdes.SpatBroadcastRateEvent()) {

            var inputTopic = driver.createInputTopic(inputTopicName,
                    rsuIntersectionKeySerde.serializer(), processedSpatSerde.serializer());
            var broadcastRateTopic = driver.createOutputTopic(broadcastRateTopicName,
                    rsuIntersectionKeySerde.deserializer(), spatBroadcastRateEventSerde.deserializer());

            final RsuIntersectionKey key = new RsuIntersectionKey(rsuId, intersectionId, region);
            for (var instant : pipeOrder) {
                inputTopic.pipeInput(key, createSpat(instant), instant);
            }

            return broadcastRateTopic.readKeyValuesToList();
        }
    }

    private List<KeyValue<RsuIntersectionKey, SpatBroadcastRateNotification>> pipeAndCollectNotifications(
            List<Instant> pipeOrder) {
        var streamsConfig = createStreamsConfig();
        Topology topology = createTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig);
             Serde<RsuIntersectionKey> rsuIntersectionKeySerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey();
             Serde<ProcessedSpat> processedSpatSerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSpat();
             Serde<SpatBroadcastRateNotification> notificationSerde = JsonSerdes.SpatBroadcastRateNotification()) {

            var inputTopic = driver.createInputTopic(inputTopicName,
                    rsuIntersectionKeySerde.serializer(), processedSpatSerde.serializer());
            var notificationTopic = driver.createOutputTopic(broadcastRateNotificationTopicName,
                    rsuIntersectionKeySerde.deserializer(), notificationSerde.deserializer());

            final RsuIntersectionKey key = new RsuIntersectionKey(rsuId, intersectionId, region);
            for (var instant : pipeOrder) {
                inputTopic.pipeInput(key, createSpat(instant), instant);
            }

            return notificationTopic.readKeyValuesToList();
        }
    }

    // Pipes spats whose embedded timestamp and odeReceivedAt diverge, per divergentInstantPairs.
    private List<KeyValue<RsuIntersectionKey, SpatBroadcastRateEvent>> pipeAndCollectDivergentEvents(
            List<Instant[]> instantPairs) {
        var streamsConfig = createStreamsConfig();
        Topology topology = createTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig);
             Serde<RsuIntersectionKey> rsuIntersectionKeySerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey();
             Serde<ProcessedSpat> processedSpatSerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSpat();
             Serde<SpatBroadcastRateEvent> spatBroadcastRateEventSerde = JsonSerdes.SpatBroadcastRateEvent()) {

            var inputTopic = driver.createInputTopic(inputTopicName,
                    rsuIntersectionKeySerde.serializer(), processedSpatSerde.serializer());
            var broadcastRateTopic = driver.createOutputTopic(broadcastRateTopicName,
                    rsuIntersectionKeySerde.deserializer(), spatBroadcastRateEventSerde.deserializer());

            final RsuIntersectionKey key = new RsuIntersectionKey(rsuId, intersectionId, region);
            for (var pair : instantPairs) {
                Instant embedded = pair[0];
                Instant odeReceivedAt = pair[1];
                inputTopic.pipeInput(key, createSpat(embedded, odeReceivedAt), embedded);
            }

            return broadcastRateTopic.readKeyValuesToList();
        }
    }

    private List<KeyValue<RsuIntersectionKey, SpatBroadcastRateNotification>> pipeAndCollectDivergentNotifications(
            List<Instant[]> instantPairs) {
        var streamsConfig = createStreamsConfig();
        Topology topology = createTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig);
             Serde<RsuIntersectionKey> rsuIntersectionKeySerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey();
             Serde<ProcessedSpat> processedSpatSerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSpat();
             Serde<SpatBroadcastRateNotification> notificationSerde = JsonSerdes.SpatBroadcastRateNotification()) {

            var inputTopic = driver.createInputTopic(inputTopicName,
                    rsuIntersectionKeySerde.serializer(), processedSpatSerde.serializer());
            var notificationTopic = driver.createOutputTopic(broadcastRateNotificationTopicName,
                    rsuIntersectionKeySerde.deserializer(), notificationSerde.deserializer());

            final RsuIntersectionKey key = new RsuIntersectionKey(rsuId, intersectionId, region);
            for (var pair : instantPairs) {
                Instant embedded = pair[0];
                Instant odeReceivedAt = pair[1];
                inputTopic.pipeInput(key, createSpat(embedded, odeReceivedAt), embedded);
            }

            return notificationTopic.readKeyValuesToList();
        }
    }

    private static List<SpatBroadcastRateEvent> filterByNumberOfMessages(
            List<KeyValue<RsuIntersectionKey, SpatBroadcastRateEvent>> events, int numberOfMessages) {
        return events.stream()
                .map(kv -> kv.value)
                .filter(event -> event.getNumberOfMessages() == numberOfMessages)
                .collect(Collectors.toList());
    }

    private static List<SpatBroadcastRateEvent> pairEvents(List<KeyValue<RsuIntersectionKey, SpatBroadcastRateEvent>> events) {
        return filterByNumberOfMessages(events, 2);
    }

    private static List<SpatBroadcastRateEvent> durationEvents(List<KeyValue<RsuIntersectionKey, SpatBroadcastRateEvent>> events) {
        return filterByNumberOfMessages(events, TimestampBoundedQueue.MAX_NUM_MESSAGES_FOR_DURATION);
    }

    private static List<SpatBroadcastRateEvent> pairEvents(
            List<KeyValue<RsuIntersectionKey, SpatBroadcastRateEvent>> events, TimestampType type) {
        return filterByType(pairEvents(events), type);
    }

    private static List<SpatBroadcastRateEvent> durationEvents(
            List<KeyValue<RsuIntersectionKey, SpatBroadcastRateEvent>> events, TimestampType type) {
        return filterByType(durationEvents(events), type);
    }

    private static List<SpatBroadcastRateEvent> filterByType(List<SpatBroadcastRateEvent> events, TimestampType type) {
        return events.stream()
                .filter(event -> event.getTimestampType() == type)
                .collect(Collectors.toList());
    }

    // Picks the earliest-window notification of the given type (there may be more than one if
    // multiple assessment windows closed, e.g. when embedded and odeReceivedAt periods diverge
    // enough that one pipeline's stream time outpaces the other's).
    private static SpatBroadcastRateNotification notificationOfType(
            List<KeyValue<RsuIntersectionKey, SpatBroadcastRateNotification>> notifications, TimestampType type) {
        return notifications.stream()
                .map(kv -> kv.value)
                .filter(notification -> notification.getAssessment().getTimestampType() == type)
                .min(Comparator.comparingLong(n -> n.getAssessment().getTimePeriod().getBeginTimestamp()))
                .orElseThrow(() -> new AssertionError("No notification found for type " + type));
    }

    private List<Instant> instantsWithPeriod(int periodMillis, int totalSeconds) {
        return TopologyTestUtils.getInstants(startTime, periodMillis, totalSeconds);
    }

    // Pairs of {embeddedTimestamp, odeReceivedAt} instants, one per spat, each advancing at its
    // own period from startTime. Count is sized off the faster (smaller-period) sequence so both
    // sequences span at least totalSeconds, regardless of which one is faster.
    private List<Instant[]> divergentInstantPairs(int embeddedPeriodMillis, int odeReceivedAtPeriodMillis, int totalSeconds) {
        int fastestPeriodMillis = Math.min(embeddedPeriodMillis, odeReceivedAtPeriodMillis);
        int count = (totalSeconds * 1000 / fastestPeriodMillis) + 5;
        var pairs = new ArrayList<Instant[]>();
        for (int i = 0; i <= count; i++) {
            pairs.add(new Instant[] {
                    startTime.plusMillis((long) i * embeddedPeriodMillis),
                    startTime.plusMillis((long) i * odeReceivedAtPeriodMillis)
            });
        }
        return pairs;
    }

    // Count of instants falling in the first assessment window: [startTime, startTime + duration).
    private long expectedSpatCount(List<Instant> instants) {
        long windowEndMillis = startTime.toEpochMilli() + v2AssessmentWindowDuration * 1000L;
        return instants.stream()
                .filter(i -> i.toEpochMilli() < windowEndMillis)
                .count();
    }

    private List<Instant> instantsWithGaps(int[] gapsMillis) {
        var instants = new ArrayList<Instant>();
        var current = startTime;
        instants.add(current);
        for (int gap : gapsMillis) {
            current = current.plusMillis(gap);
            instants.add(current);
        }
        return instants;
    }

    private List<Instant> withNominalTail(List<Instant> head, int totalSeconds) {
        var result = new ArrayList<>(head);
        var current = result.get(result.size() - 1);
        while (Duration.between(startTime, current).getSeconds() <= totalSeconds) {
            current = current.plusMillis(100);
            result.add(current);
        }
        return result;
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

    private List<Instant> withFirstWindowReversed(List<Instant> instants) {
        int windowBoundaryIndex = 0;
        while (windowBoundaryIndex < instants.size()
                && Duration.between(startTime, instants.get(windowBoundaryIndex)).toMillis() < v2BufferSizeSeconds * 1000L) {
            windowBoundaryIndex++;
        }
        var firstWindow = new ArrayList<>(instants.subList(0, windowBoundaryIndex));
        Collections.reverse(firstWindow);

        var result = new ArrayList<>(firstWindow);
        result.addAll(instants.subList(windowBoundaryIndex, instants.size()));
        return result;
    }

    private Topology createTopology() {
        var parameters = getParameters();
        var spatValidationTopology = new SpatValidationTopologyV2();
        spatValidationTopology.setParameters(parameters);
        // Not under test; mock rather than wiring a real SpatTimestampDeltaTopology.
        var timestampDeltaAlgorithm = Mockito.mock(SpatTimestampDeltaStreamsAlgorithm.class);
        spatValidationTopology.setTimestampDeltaAlgorithm(timestampDeltaAlgorithm);
        return spatValidationTopology.buildTopology();
    }

    private Properties createStreamsConfig() {
        var streamsConfig = new Properties();
        streamsConfig.setProperty(
                StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                TimestampExtractorForBroadcastRate.class.getName());
        return streamsConfig;
    }

    private SpatValidationParameters getParameters() {
        var parameters = new SpatValidationParameters();
        parameters.setInputTopicName(inputTopicName);
        parameters.setBroadcastRateTopicName(broadcastRateTopicName);
        parameters.setBroadcastRateNotificationTopicName(broadcastRateNotificationTopicName);
        parameters.setMinimumDataTopicName(minimumDataTopicName);
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
        parameters.setV2BroadcastRateMaxOutlierPairSeparationMs(v2MaxOutlierPairSeparationMs);
        return parameters;
    }

    private ProcessedSpat createSpat(Instant timestamp) {
        return createSpat(timestamp, timestamp);
    }

    private ProcessedSpat createSpat(Instant embeddedTimestamp, Instant odeReceivedAt) {
        var spat = new ProcessedSpat();
        spat.setUtcTimeStamp(embeddedTimestamp.atZone(ZoneOffset.UTC));
        spat.setOdeReceivedAt(odeReceivedAt.atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
        spat.setCti4501Conformant(true);
        return spat;
    }

}
