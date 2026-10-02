package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.spat_message_count_progression.SpatMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.SpatMessageCountProgressionEvent;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class SpatMessageCountProgressionTopologyTest {

    private static final String inputTopicName = "topic.ProcessedSpat";
    private static final String eventOutputTopicName = "topic.CmSpatMessageCountProgressionEvents";
    private static final String processedSpatStateStoreName = "processedSpatStateStore";
    private static final String latestSpatStateStoreName = "latestSpatStateStore";
    private static final int bufferTimeMs = 500;
    private static final int bufferGracePeriodMs = 50;
    private static final boolean debug = true;
    private static final boolean aggregateEvents = false;

    final String rsuId = "127.18.0.1";
    final int intersectionId = 12112;
    final int region = -1;

    @Test
    public void testSameContentSameRevisionProducesNoEvent() throws IOException {
        var spatA = loadSampleSpat();
        var spatB = loadSampleSpat();
        testTopology(spatA, spatB, false);
    }

    @Test
    public void testSameContentSameRevisionDifferentMetadataProducesNoEvent() throws IOException {
        var spatA = loadSampleSpat();
        var spatB = loadSampleSpat();
        spatB.setOriginIp("10.10.10.10");
        spatB.setAsn1("00123456789a");
        spatB.setValidationMessages(null);
        testTopology(spatA, spatB, false);
    }

    @Test
    public void testDifferentContentRevisionIncrementedByOneProducesNoEvent() throws IOException {
        var spatA = loadSampleSpat();
        var spatB = loadSampleSpat();
        toggleEventState(spatB);
        spatB.setRevision(spatA.getRevision() + 1);
        testTopology(spatA, spatB, false);
    }

    @Test
    public void testDifferentContentRevisionIncrementedByOneWithRolloverProducesNoEvent() throws IOException {
        var spatA = loadSampleSpat();
        var spatB = loadSampleSpat();
        spatA.setRevision(127);
        spatB.setRevision(0);
        toggleEventState(spatB);
        testTopology(spatA, spatB, false);
    }

    @Test
    public void testDifferentContentSameRevisionProducesEvent() throws IOException {
        var spatA = loadSampleSpat();
        var spatB = loadSampleSpat();
        toggleEventState(spatB);
        testTopology(spatA, spatB, true);
    }

    @Test
    public void testDifferentContentTimingChangedSameRevisionProducesEvent() throws IOException {
        var spatA = loadSampleSpat();
        var spatB = loadSampleSpat();
        changeTiming(spatB);
        testTopology(spatA, spatB, true);
    }

    @Test
    public void testSameContentDifferentRevisionProducesEvent() throws IOException {
        var spatA = loadSampleSpat();
        var spatB = loadSampleSpat();
        spatB.setRevision(spatA.getRevision() + 1);
        testTopology(spatA, spatB, true);
    }

    // max timestamp offset 150ms
    @Test
    public void testSameContentDifferentRevisionTimestampOffsetGreaterThanMaxProducesNoEvent() throws IOException {
        var spatA = loadSampleSpat();
        var spatB = loadSampleSpat();
        spatB.setRevision(spatA.getRevision() + 1);
        testTopology(spatA, spatB, false, 200);
    }

    @Test
    public void testDifferentContentRevisionIncrementedByTwoProducesEvent() throws IOException {
        var spatA = loadSampleSpat();
        var spatB = loadSampleSpat();
        toggleEventState(spatB);
        spatB.setRevision(spatA.getRevision() + 2);
        testTopology(spatA, spatB, true);
    }

    private void toggleEventState(ProcessedSpat spat) {
        var event = spat.getStates().getFirst().getStateTimeSpeed().getFirst();
        event.setEventState(event.getEventState() == ProcessedMovementPhaseState.STOP_AND_REMAIN
                ? ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED
                : ProcessedMovementPhaseState.STOP_AND_REMAIN);
    }

    private void changeTiming(ProcessedSpat spat) {
        var timing = spat.getStates().getFirst().getStateTimeSpeed().getFirst().getTiming();
        timing.setMinEndTime(timing.getMinEndTime().plusSeconds(5));
        timing.setMaxEndTime(timing.getMaxEndTime().plusSeconds(5));
    }

    private void testTopology(ProcessedSpat spatA, ProcessedSpat spatB, boolean expectEvent) {
        testTopology(spatA, spatB, expectEvent, 100);
    }

    private void testTopology(ProcessedSpat spatA, ProcessedSpat spatB, boolean expectEvent, long timeBOffsetMs) {
        Topology topology = createTopology();
        try (TopologyTestDriver driver = new TopologyTestDriver(topology);
             Serde<RsuIntersectionKey> keySerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey();
             Serde<ProcessedSpat> processedSpatSerdes
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSpat();
             Serde<SpatMessageCountProgressionEvent> eventSerde = JsonSerdes.SpatMessageCountProgressionEvent();
        ) {
            var inputTopic = driver.createInputTopic(inputTopicName,
                    keySerde.serializer(),
                    processedSpatSerdes.serializer());

            var outputTopic = driver.createOutputTopic(eventOutputTopicName,
                    keySerde.deserializer(),
                    eventSerde.deserializer());

            final Instant startTime = Instant.ofEpochMilli(1674356320000L);
            final Instant timeA = startTime.plusMillis(100L);
            final Instant timeB = timeA.plusMillis(timeBOffsetMs);
            // Flush ping, far enough past buffer + grace period (relative to timeA) and past the
            // grace period cutoff relative to timeB so the buffered spatA -> spatB pair gets processed.
            final Instant timeFinal = timeA.plusMillis(timeBOffsetMs + bufferTimeMs + bufferGracePeriodMs + 100L);

            final RsuIntersectionKey key = new RsuIntersectionKey(rsuId, intersectionId, region);

            // Baseline spat
            spatA.setUtcTimeStamp(ZonedDateTime.ofInstant(timeA, ZoneOffset.UTC));
            inputTopic.pipeInput(key, spatA, timeA);

            // Possibly changed spat
            spatB.setUtcTimeStamp(ZonedDateTime.ofInstant(timeB, ZoneOffset.UTC));
            inputTopic.pipeInput(key, spatB, timeB);

            // Send changed spat again to advance stream time beyond the buffer + grace period to get event to be produced
            spatB.setUtcTimeStamp(ZonedDateTime.ofInstant(timeFinal, ZoneOffset.UTC));
            inputTopic.pipeInput(key, spatB, timeFinal);

            var events = outputTopic.readKeyValuesToList();
            if (expectEvent) {
                assertThat("expected event", events, hasSize(1));
            } else {
                assertThat("unexpected event", events, hasSize(0));
            }
        }
    }

    private Topology createTopology() {
        var parameters = getParameters();
        var spatValidationTopology = new SpatMessageCountProgressionTopology();
        spatValidationTopology.setParameters(parameters);
        return spatValidationTopology.buildTopology();
    }

    private SpatMessageCountProgressionParameters getParameters() {
        var parameters = new SpatMessageCountProgressionParameters();
        parameters.setDebug(debug);
        parameters.setSpatInputTopicName(inputTopicName);
        parameters.setSpatMessageCountProgressionEventOutputTopicName(eventOutputTopicName);
        parameters.setProcessedSpatStateStoreName(processedSpatStateStoreName);
        parameters.setLatestSpatStateStoreName(latestSpatStateStoreName);
        parameters.setBufferTimeMs(bufferTimeMs);
        parameters.setBufferGracePeriodMs(bufferGracePeriodMs);
        parameters.setAggregateEvents(aggregateEvents);
        return parameters;
    }

    private static final String SAMPLE_SPAT_RESOURCE = "/us/dot/its/jpo/conflictmonitor/monitor/topologies/sample.processed-spat.json";

    private ProcessedSpat loadSampleSpat() throws IOException {
        try (InputStream in = getClass().getResourceAsStream(SAMPLE_SPAT_RESOURCE);
             Serde<ProcessedSpat> serde = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSpat()) {
            assertThat(in, notNullValue());
            byte[] bytes = IOUtils.toByteArray(in);
            return serde.deserializer().deserialize("test-topic", bytes);
        }
    }
}
