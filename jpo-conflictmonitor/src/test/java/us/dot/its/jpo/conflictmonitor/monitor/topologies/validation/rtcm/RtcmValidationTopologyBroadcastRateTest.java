package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.rtcm;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate.RtcmBroadcastRateEvent;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.conflictmonitor.testutils.TopologyTestUtils;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.runners.Parameterized.*;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

@Slf4j
@RunWith(Parameterized.class)
public class RtcmValidationTopologyBroadcastRateTest extends BaseRtcmValidationTopologyTest {

    @Parameters(name = "{index}: {0}")
    public static Collection<Object[]> data() {
        var args = new Object[][]{
                {"rtcm basic message types slow broadcast rate, .5 Hz", false, 2000, 13, true, false,},
                {"rtcm basic message types fast broadcast rate 2 Hz", false, 500, 11, false, true},
                {"rtcm basic message types correct broadcast rate 1Hz", false, 1000, 11, false, false},
                {"rtcm MSM 4 message types slow broadcast rate, .5 Hz", false, 2000, 13, true, false},
                {"rtcm MSM 4 message types fast broadcast rate 20 Hz", false, 50, 11, false, true},
                {"rtcm MSM 4 message types correct broadcast rate 1 Hz", false, 1000, 11, false, false},
                {"rtcm MSM 4 message types correct broadcast rate 5 Hz", false, 200, 11, false, false},
                {"rtcm MSM 4 message types correct broadcast rate 10 Hz", false, 100, 11, false, false}
        };
        return Arrays.asList(args);
    }

    private final String description;
    private final boolean includeMsm;
    private final int periodMillis;
    private final int totalTimeSeconds;gi
    private final boolean expectLow;
    private final boolean expectHigh;

    public RtcmValidationTopologyBroadcastRateTest(String description, boolean includeMsm, int periodMillis, int totalTimeSeconds,
         boolean expectLow, boolean expectHigh) {
        this.description = description;
        this.includeMsm = includeMsm;
        this.periodMillis = periodMillis;
        this.totalTimeSeconds = totalTimeSeconds;
        this.expectLow = expectLow;
        this.expectHigh = expectHigh;
    }

    @Test
    public void testRtcmValidationTopologyBroadcastRate() {
        log.info(description);
        Properties streamsConfig = createStreamsConfig();
        Topology topology = createTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig);
             Serde<ProcessedRTCM> processedRTCMSerde = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedRTCM();
             Serde<RsuStationIdKey> keySerde = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey();
             Serde<RtcmBroadcastRateEvent> broadcastRateEventSerde = JsonSerdes.RtcmBroadcastRateEvent()) {

            var inputTopic = driver.createInputTopic(inputTopicName,
                    keySerde.serializer(), processedRTCMSerde.serializer());

            var broadcastRateTopic = driver.createOutputTopic(broadcastRateTopicName,
                    keySerde.deserializer(),  broadcastRateEventSerde.deserializer());

            final RsuStationIdKey key = new RsuStationIdKey();
            key.setRsuId(rsuId);
            key.setStationId(stationId);

            List<Instant> instants = TopologyTestUtils.getInstants(startTime, periodMillis, totalTimeSeconds);
            log.info("num instants {}", instants.size());
            for (var currentInstant : instants) {
                var rtcm = createRtcm(currentInstant, includeMsm);
                inputTopic.pipeInput(key, rtcm, currentInstant);
            }

            var broadcastRateList = broadcastRateTopic.readKeyValuesToList();
            if (expectLow || expectHigh) {
                assertThat("Should be 1 broadcast rate event", broadcastRateList, hasSize(1));
                var broadcastRate = broadcastRateList.getFirst();
                var bcKey = broadcastRate.key;
                assertThat("broadcast rate key rsuId", bcKey.getRsuId(), equalTo(rsuId));
                assertThat("broadcast rate key stationId", bcKey.getStationId(), equalTo(stationId));
                var bcValue = broadcastRate.value;
                assertThat("broadcast rate device id", bcValue.getSource(), equalTo(source));
                assertThat("broadcast rate stationId", bcValue.getStationId(), equalTo(stationId));
                assertThat("broadcast rate topic name", bcValue.getTopicName(), equalTo(inputTopicName));
                if (expectLow) {
                    assertThat("expect low broadcast rate number of messages", bcValue.getNumberOfMessages(), lessThan(lowerBound));
                } else {
                    assertThat("expect high number of messages", bcValue.getNumberOfMessages(), greaterThan(upperBound));
                }
                assertThat("broadcast rate time period null", bcValue.getTimePeriod(), notNullValue());
                assertThat("broadcast rate time period", bcValue.getTimePeriod().periodMillis(), equalTo(10000L));
            } else {
                assertThat("Expect 0 broadcast rate events", broadcastRateList, hasSize(0));
            }
        }
    }



}
