package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression.RtcmMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.RtcmMessageCountProgressionEvent;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.aggregation.RtcmMessageCountProgressionAggregationTopology;
import us.dot.its.jpo.conflictmonitor.testutils.ResourceUtils;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;
import us.dot.its.jpo.geojsonconverter.serialization.deserializers.JsonDeserializer;
import us.dot.its.jpo.geojsonconverter.serialization.serializers.JsonSerializer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


@Slf4j
@RunWith(Parameterized.class)
public class RtcmMessageCountProgressionTopologyTest {

    // Initialize mockito annotations
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    // Mock the aggregation subtopology tested separately from this test
    @Mock
    RtcmMessageCountProgressionAggregationTopology mockAggregationTopology;

    // Test various combinations of buffer time, grace period and punctuate time
    @Parameterized.Parameters(name =
            "{index}: buf={0} grace={1} check={2} msgCntA={3} msgCntB={4} propA={5} propB={6}")
    public static Collection<Object[]> testParams() {
        final double prop1 = 123.4d;
        final double prop2 = 321.0d;
        return Arrays.asList(new Object[][] {
                {5000, 1200, 500, 10, 11, prop1, prop1},
                {5000, 500, 500, 10, 11, prop1, prop1},
                {5000, 500, 1000, 10, 11, prop1, prop1},
                {5000, 500, 2500, 10, 11, prop1, prop1},
                {2000, 500, 500, 10, 11, prop1, prop1},
                {5000, 0, 500, 10, 11, prop1, prop1},
                {5000, 1200, 500, 2, 2, prop1, prop1},
                {5000, 500, 500, 2, 2, prop1, prop1},
                {5000, 500, 1000, 2, 2, prop1, prop1},
                {5000, 500, 2500, 2, 2, prop1, prop1},
                {2000, 500, 500, 2, 2, prop1, prop1},
                {5000, 0, 500, 2, 2, prop1, prop1},
                {5000, 1200, 500, 10, 11, prop1, prop2},
                {5000, 500, 500, 10, 11, prop1, prop2},
                {5000, 500, 1000, 10, 11, prop1, prop2},
                {5000, 500, 2500, 10, 11, prop1, prop2},
                {2000, 500, 500, 10, 11, prop1, prop2},
                {5000, 0, 500, 10, 11, prop1, prop2},
                {5000, 1200, 500, 2, 2, prop1, prop2},
                {5000, 500, 500, 2, 2, prop1, prop2},
                {5000, 500, 1000, 2, 2, prop1, prop2},
                {5000, 500, 2500, 2, 2, prop1, prop2},
                {2000, 500, 500, 2, 2, prop1, prop2},
                {5000, 0, 500, 2, 2, prop1, prop2},
        });
    }

    public RtcmMessageCountProgressionTopologyTest(int bufferTimeMs, int bufferGracePeriodMs, int checkIntervalMs,
                                                   int msgCntA, int msgCntB, double propA, double propB) {
        this.bufferTimeMs = bufferTimeMs;
        this.bufferGracePeriodMs = bufferGracePeriodMs;
        this.checkIntervalMs = checkIntervalMs;
        this.msgCntA = msgCntA;
        this.msgCntB = msgCntB;
        this.propA = propA;
        this.propB = propB;

        // Expect event if msg count changes with unchanged contents
        // or msg count doesn't change, but contents do
        this.expectEvent = ((msgCntA != msgCntB) && (propA == propB))
            || ((msgCntA == msgCntB) && (propA != propB));
    }

    private final int bufferTimeMs;
    private final int bufferGracePeriodMs;
    private final int checkIntervalMs;
    private final int msgCntA;
    private final int msgCntB;
    private final boolean expectEvent;
    private final double propA;
    private final double propB;

    private final String rtcmInputTopicName = "topic.ProcessedRtcm";
    private final String eventOutputTopicName = "topic.CmRtcmMessageCountProgressionEvents";
    private final String processedRtcmStateStoreName = "processedRtcmStateStore";
    private final String latestRtcmStateStoreName = "latestRtcmStateStore";
    private final String latestEventStateStoreName = "latestEventStateStore";


    private final String rsuId = "127.0.0.1";
    private final int stationId = 2432;


    private final long startTimestamp = 1750000000000L;

    @Test
    public void testRtcmMessageCountTopology() throws JsonProcessingException {
        Properties streamsConfig = createStreamsConfig();
        Topology topology = createTopology();
        final Instant startTime = Instant.ofEpochMilli(startTimestamp);
        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig, startTime)) {

            var rtcmInputTopic = driver.createInputTopic(rtcmInputTopicName,
                    new JsonSerializer<RsuStationIdKey>(),
                    new JsonSerializer<ProcessedRTCM>());

            var eventOutputTopic = driver.createOutputTopic(eventOutputTopicName,
                    new JsonDeserializer<>(RsuStationIdKey.class),
                    new JsonDeserializer<>(RtcmMessageCountProgressionEvent.class));

            final var key = new RsuStationIdKey();
            key.setRsuId(rsuId);
            key.setStationId(stationId);

            final long diffMs = 1000;
            final Duration timeDiff = Duration.ofMillis(diffMs);
            final Instant nextTime = startTime.plus(timeDiff);
            final long nextTimestamp = nextTime.toEpochMilli();

            var rtcm1 = getProcessedRTCM_type1004(msgCntA, propA, startTimestamp);
            var rtcm2 = getProcessedRTCM_type1004(msgCntB, propB, nextTimestamp);

            rtcmInputTopic.pipeInput(key, rtcm1, startTime);

            long time = 0L;
            time += diffMs;
            driver.advanceWallClockTime(timeDiff);

            rtcmInputTopic.pipeInput(key, rtcm2, nextTime);

            // Run out the clock to get an event out
            while (time < bufferTimeMs + bufferGracePeriodMs) {
                time += checkIntervalMs;
                driver.advanceWallClockTime(timeDiff);
                log.info("relative wall clock time: {}", time);
            }

            var eventList = eventOutputTopic.readKeyValuesToList();
            if (expectEvent) {
                assertThat(eventList, hasSize(equalTo(1)));
                KeyValue<RsuStationIdKey, RtcmMessageCountProgressionEvent> kv = eventList.getFirst();
                RsuStationIdKey resultKey = kv.key;
                assertThat(resultKey.getRsuId(), equalTo(rsuId));
                assertThat(resultKey.getStationId(), equalTo(stationId));
                RtcmMessageCountProgressionEvent event = kv.value;
                assertThat(event.getMessageCountA(), equalTo(msgCntA));
                assertThat(event.getMessageCountB(), equalTo(msgCntB));
                if (propA == propB) {
                    assertThat(event.getChange(), hasSize(equalTo(0)));
                    assertThat(msgCntA, not(equalTo(msgCntB)));
                } else {
                    assertThat(event.getChange(), hasSize(greaterThan(0)));
                    assertThat(msgCntA, equalTo(msgCntB));
                }
                assertThat(event.getStationId(), equalTo(stationId));
                assertThat(event.getTimestampA(), equalTo(startTimestamp));
                assertThat(event.getTimestampB(), equalTo(nextTimestamp));
            } else {
                assertThat(eventList, hasSize(equalTo(0)));
            }
        }
    }

    private Properties createStreamsConfig() {
        Properties streamsConfig = new Properties();
        return streamsConfig;
    }

    private Topology createTopology() {
        var parameters = getParameters();
        var topology = new RtcmMessageCountProgressionTopology();
        topology.setParameters(parameters);
        topology.setAggregationAlgorithm(mockAggregationTopology);
        return topology.buildTopology();
    }

    private RtcmMessageCountProgressionParameters getParameters() {
        var parameters = new RtcmMessageCountProgressionParameters();
        parameters.setDebug(false);
        parameters.setRtcmInputTopicName(rtcmInputTopicName);
        parameters.setRtcmMessageCountProgressionOutputTopicName(eventOutputTopicName);
        parameters.setProcessedRtcmStateStoreName(processedRtcmStateStoreName);
        parameters.setLatestRtcmStateStoreName(latestRtcmStateStoreName);
        parameters.setLatestEventStateStoreName(latestEventStateStoreName);
        parameters.setBufferTimeMs(bufferTimeMs);
        parameters.setBufferGracePeriodMs(bufferGracePeriodMs);
        parameters.setCheckIntervalMs(checkIntervalMs);
        parameters.setAggregateEvents(false);
        return parameters;
    }


    private ProcessedRTCM getProcessedRTCM_type1004(final int msgCnt, final double prange, final long timestamp) throws JsonProcessingException {
        ProcessedRTCM rtcm = loadProcessedRTCM("processed-rtcm-with-1004.json");
        rtcm.getProperties().setMsgCnt(msgCnt);
        rtcm.getProperties().setUtcTime(timestamp);
        rtcm.getProperties().setOdeReceivedAt(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC));
        // change the first "prange" value
        JsonNode l1 = rtcm.getProperties().getMessages().get(2).getDecodedMessage().get("satellites").get(0).get("L1");
        if (l1 instanceof ObjectNode objNode) {
            objNode.put("prange", prange);
        }
        return rtcm;
    }


    private static final String RESOURCE_PATH = "/us/dot/its/jpo/conflictmonitor/monitor/utils/";

    private static ProcessedRTCM loadProcessedRTCM(final String resourceName) throws JsonProcessingException {
        String spatStr = ResourceUtils.loadResource(RESOURCE_PATH + resourceName);
        ObjectMapper mapper = DateJsonMapper.getInstance();
        return mapper.readValue(spatStr, ProcessedRTCM.class);
    }
}

