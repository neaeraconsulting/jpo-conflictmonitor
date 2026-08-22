package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.bsm_message_count_progression.BsmMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.BsmMessageCountProgressionEvent;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuLogKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.bsm.ProcessedBsm;
import us.dot.its.jpo.geojsonconverter.serialization.deserializers.ProcessedBsmDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class BsmMessageCountProgressionTopologyTest {

    private static final String inputTopicName = "topic.ProcessedBsm";
    private static final String eventOutputTopicName = "topic.CmBsmMessageCountProgressionEvents";
    private static final String processedBsmStateStoreName = "processedBsmStateStore";
    private static final String latestBsmStateStoreName = "latestBsmStateStore";
    private static final int bufferTimeMs = 500;
    private static final int bufferGracePeriodMs = 50;
    private static final boolean debug = true;
    private static final boolean aggregateEvents = false;

    final String rsuId = "127.18.0.1";
    final String logId = "logId1";
    final String bsmId = "31325433";

    @Test
    public void testSameContentSameCountProducesNoEvent() throws IOException {
        var bsmA = loadSampleBsm();
        var bsmB = loadSampleBsm();
        testTopology(bsmA, bsmB, false);
    }

    @Test
    public void testSameContentSameCountDifferentMetadataProducesNoEvent() throws IOException {
        var bsmA = loadSampleBsm();
        var bsmB = loadSampleBsm();
        var propertiesB = bsmB.getProperties();
        propertiesB.setOriginIp("10.10.10.10");
        propertiesB.setAsn1("00123456789a");
        propertiesB.setLogName("other.log");
        propertiesB.setValidationMessages(null);
        testTopology(bsmA, bsmB, false);
    }

    @Test
    public void testDifferentContentCountIncrementedByOneProducesNoEvent() throws IOException {
        var bsmA = loadSampleBsm();
        var bsmB = loadSampleBsm();
        changeSpeed(bsmB);
        bsmB.getProperties().setMsgCnt(bsmA.getProperties().getMsgCnt() + 1);
        testTopology(bsmA, bsmB, false);
    }

    @Test
    public void testDifferentContentCountIncrementedByOneWithRolloverProducesNoEvent() throws IOException {
        var bsmA = loadSampleBsm();
        var bsmB = loadSampleBsm();
        bsmA.getProperties().setMsgCnt(127);
        bsmB.getProperties().setMsgCnt(0);
        changeSpeed(bsmB);
        testTopology(bsmA, bsmB, false);
    }

    @Test
    public void testDifferentContentSameCountProducesEvent() throws IOException {
        var bsmA = loadSampleBsm();
        var bsmB = loadSampleBsm();
        changeSpeed(bsmB);
        testTopology(bsmA, bsmB, true);
    }

    @Test
    public void testDifferentContentHeadingChangedSameCountProducesEvent() throws IOException {
        var bsmA = loadSampleBsm();
        var bsmB = loadSampleBsm();
        changeHeading(bsmB);
        testTopology(bsmA, bsmB, true);
    }

    @Test
    public void testSameContentDifferentCountProducesEvent() throws IOException {
        var bsmA = loadSampleBsm();
        var bsmB = loadSampleBsm();
        bsmB.getProperties().setMsgCnt(bsmA.getProperties().getMsgCnt() + 1);
        testTopology(bsmA, bsmB, true);
    }

    // max timestamp offset 150ms
    @Test
    public void testSameContentDifferentCountTimestampOffsetGreaterThanMaxProducesNoEvent() throws IOException {
        var bsmA = loadSampleBsm();
        var bsmB = loadSampleBsm();
        bsmB.getProperties().setMsgCnt(bsmA.getProperties().getMsgCnt() + 1);
        testTopology(bsmA, bsmB, false, 200);
    }

    @Test
    public void testDifferentContentCountIncrementedByTwoProducesEvent() throws IOException {
        var bsmA = loadSampleBsm();
        var bsmB = loadSampleBsm();
        changeSpeed(bsmB);
        bsmB.getProperties().setMsgCnt(bsmA.getProperties().getMsgCnt() + 2);
        testTopology(bsmA, bsmB, true);
    }

    private void changeSpeed(ProcessedBsm<Point> bsm) {
        var properties = bsm.getProperties();
        properties.setSpeed(properties.getSpeed() + 5.0);
    }

    private void changeHeading(ProcessedBsm<Point> bsm) {
        var properties = bsm.getProperties();
        properties.setHeading(properties.getHeading() + 10.0);
    }

    /**
     * Sets timeStamp, odeReceivedAt and secMark consistently from a single Instant, so that
     * BsmTimestampExtractor (which derives the record timestamp from odeReceivedAt + secMark,
     * not timeStamp) produces exactly this instant.
     */
    private void setBsmTimestamp(ProcessedBsm<Point> bsm, Instant instant) {
        var properties = bsm.getProperties();
        ZonedDateTime zdt = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
        properties.setTimeStamp(zdt);
        properties.setOdeReceivedAt(zdt.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        properties.setSecMark(zdt.getSecond() * 1000 + zdt.getNano() / 1_000_000);
    }

    private void testTopology(ProcessedBsm<Point> bsmA, ProcessedBsm<Point> bsmB, boolean expectEvent) {
        testTopology(bsmA, bsmB, expectEvent, 100);
    }

    private void testTopology(ProcessedBsm<Point> bsmA, ProcessedBsm<Point> bsmB, boolean expectEvent, long timeBOffsetMs) {
        Topology topology = createTopology();
        try (TopologyTestDriver driver = new TopologyTestDriver(topology);
             Serde<RsuLogKey> keySerde
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuLogKey();
             Serde<ProcessedBsm<Point>> processedBsmSerdes
                     = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedBsm();
             Serde<BsmMessageCountProgressionEvent> eventSerde = JsonSerdes.BsmMessageCountProgressionEvent();
        ) {
            var inputTopic = driver.createInputTopic(inputTopicName,
                    keySerde.serializer(),
                    processedBsmSerdes.serializer());

            var outputTopic = driver.createOutputTopic(eventOutputTopicName,
                    keySerde.deserializer(),
                    eventSerde.deserializer());

            final Instant startTime = Instant.ofEpochMilli(1674356320000L);
            final Instant timeA = startTime.plusMillis(100L);
            final Instant timeB = timeA.plusMillis(timeBOffsetMs);
            // Flush ping, far enough past buffer + grace period (relative to timeA) and past the
            // grace period cutoff relative to timeB so the buffered bsmA -> bsmB pair gets processed.
            final Instant timeFinal = timeA.plusMillis(timeBOffsetMs + bufferTimeMs + bufferGracePeriodMs + 100L);

            final RsuLogKey key = new RsuLogKey(rsuId, logId, bsmId);

            // Baseline bsm
            setBsmTimestamp(bsmA, timeA);
            inputTopic.pipeInput(key, bsmA, timeA);

            // Possibly changed bsm
            setBsmTimestamp(bsmB, timeB);
            inputTopic.pipeInput(key, bsmB, timeB);

            // Send changed bsm again to advance stream time beyond the buffer + grace period to get event to be produced
            setBsmTimestamp(bsmB, timeFinal);
            inputTopic.pipeInput(key, bsmB, timeFinal);

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
        var bsmValidationTopology = new BsmMessageCountProgressionTopology();
        bsmValidationTopology.setParameters(parameters);
        return bsmValidationTopology.buildTopology();
    }

    private BsmMessageCountProgressionParameters getParameters() {
        var parameters = new BsmMessageCountProgressionParameters();
        parameters.setDebug(debug);
        parameters.setBsmInputTopicName(inputTopicName);
        parameters.setBsmMessageCountProgressionEventOutputTopicName(eventOutputTopicName);
        parameters.setProcessedBsmStateStoreName(processedBsmStateStoreName);
        parameters.setLatestBsmStateStoreName(latestBsmStateStoreName);
        parameters.setBufferTimeMs(bufferTimeMs);
        parameters.setBufferGracePeriodMs(bufferGracePeriodMs);
        parameters.setAggregateEvents(aggregateEvents);
        return parameters;
    }

    private static final String SAMPLE_BSM_RESOURCE = "/us/dot/its/jpo/conflictmonitor/monitor/topologies/sample.processed-bsm.json";

    private ProcessedBsm<Point> loadSampleBsm() throws IOException {
        try (InputStream in = getClass().getResourceAsStream(SAMPLE_BSM_RESOURCE);
             var deserializer = new ProcessedBsmDeserializer<>(Point.class)) {
            assertThat(in, notNullValue());
            byte[] bytes = IOUtils.toByteArray(in);
            return deserializer.deserialize("test-topic", bytes);
        }
    }
}
