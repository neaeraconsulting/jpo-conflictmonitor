package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression.RtcmMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.RtcmMessageCountProgressionEvent;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.aggregation.RtcmMessageCountProgressionAggregationTopology;
import us.dot.its.jpo.conflictmonitor.testutils.ResourceUtils;
import us.dot.its.jpo.geojsonconverter.DateJsonMapper;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.RTCMProperties;
import us.dot.its.jpo.geojsonconverter.serialization.deserializers.JsonDeserializer;
import us.dot.its.jpo.geojsonconverter.serialization.serializers.JsonSerializer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;


@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class RtcmMessageCountProgressionTopologyTest {

    // Mock the aggregation subtopology tested seperately from this test
    @Mock
    RtcmMessageCountProgressionAggregationTopology mockAggregationTopology;

    private final String rtcmInputTopicName = "topic.ProcessedRtcm";
    private final String eventOutputTopicName = "topic.CmRtcmMessageCountProgressionEvents";
    private final String processedRtcmStateStoreName = "processedRtcmStateStore";
    private final String latestRtcmStateStoreName = "latestRtcmStateStore";
    private final int bufferTimeMs = 500;
    private final int bufferGracePriodMs = 50;

    private final String rsuId = "127.0.0.1";
    private final int stationId = 2432;


    private final long startTimestamp = 1750000000000L;

    @Test
    public void testRtcmMessageCountTopology() throws JsonProcessingException {
        Properties streamsConfig = createStreamsConfig();
        Topology topology = createTopology();
        Instant startTime = Instant.ofEpochMilli(startTimestamp);
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


            final int msgCntA = 10;
            final int msgCntB = 11;

            Instant nextTime = startTime.plusSeconds(1);
            final long nextTimestamp = nextTime.toEpochMilli();
            var rtcm1 = getProcessedRTCM_type1004(msgCntA, 123.4, startTimestamp);
            var rtcm2 = getProcessedRTCM_type1004(msgCntB, 123.4, nextTimestamp);
            rtcmInputTopic.pipeInput(key, rtcm1, startTime);

            rtcmInputTopic.pipeInput(key, rtcm2, nextTime);
            Duration timeDiff = Duration.between(startTime, nextTime);
            driver.advanceWallClockTime(timeDiff);
            driver.advanceWallClockTime(timeDiff);


            var eventList = eventOutputTopic.readKeyValuesToList();
            assertThat(eventList, hasSize(equalTo(1)));
            KeyValue<RsuStationIdKey, RtcmMessageCountProgressionEvent> kv = eventList.getFirst();
            RsuStationIdKey resultKey = kv.key;
            assertThat(resultKey.getRsuId(), equalTo(rsuId));
            assertThat(resultKey.getStationId(), equalTo(stationId));
            RtcmMessageCountProgressionEvent event = kv.value;
            assertThat(event.getMessageCountA(), equalTo(msgCntA));
            assertThat(event.getMessageCountB(), equalTo(msgCntB));
            assertThat(event.getChange(), hasSize(equalTo(0)));
            assertThat(event.getStationId(), equalTo(stationId));
            assertThat(event.getTimestampA(), equalTo(startTimestamp));
            assertThat(event.getTimestampB(), equalTo(nextTimestamp));
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
        parameters.setBufferTimeMs(bufferTimeMs);
        parameters.setBufferGracePeriodMs(bufferGracePriodMs);
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
            objNode.put("prance", prange);
        }
        return rtcm;
    }

    private ProcessedRTCM getProcessedRTCM_MSM4(final int msgCnt, final int iods, final long timestamp) throws JsonProcessingException {
        ProcessedRTCM rtcm = loadProcessedRTCM("processed-rtcm-with-MSM4.json");
        rtcm.getProperties().setUtcTime(timestamp);
        rtcm.getProperties().setOdeReceivedAt(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC));
        rtcm.getProperties().setMsgCnt(msgCnt);
        // Change the "IODS" property in the decoded message
        JsonNode decodedMessage = rtcm.getProperties().getMessages().get(2).getDecodedMessage();
        if (decodedMessage instanceof ObjectNode objNode) {
            objNode.put("IODS", iods);
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

