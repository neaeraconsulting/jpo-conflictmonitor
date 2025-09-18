package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.rtcm.RtcmValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.broadcast_rate.RtcmBroadcastRateEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.RtcmMinimumDataEvent;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.conflictmonitor.testutils.TopologyTestUtils;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;
import us.dot.its.jpo.geojsonconverter.pojos.ProcessedValidationMessage;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.RTCMProperties;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@Slf4j
public class RtcmValidationTopologyTest {

    final String inputTopicName = "topic.ProcessedRtcm";
    final String broadcastRateTopicName = "topic.CmRtcmBroadcastRateEvents";
    final String minimumDataTopicName = "topic.CmRtcmMinimumDataEvents";
    final int retentionTimeMinutes = 60;
    final String notificationTopicName = "topic.CmTimestampDeltaNotification";

    // Use a tumbling window for test (rolling period = output interval)
    // just to make it easier to design the test.
    final int rollingPeriod = 10;
    final int outputInterval = 10;
    final int gracePeriod = 100;

    // Start time on 10-second window boundary
    final Instant startTime = Instant.ofEpochMilli(1674356320000L);

    final int lowerBound = 9;
    final int upperBound = 11;
    final boolean debug = true;

    final String validationMsg = "Validation Message";


    final String rsuId = "127.0.0.1";
    final int stationId = 1001;
    final String source = "{ rsuId='127.0.0.1', intersectionId='11111', region='10'}";


    @Test
    public void testRtcmValidationTopology() {

        Properties streamsConfig = createStreamsConfig();
        Topology topology = createTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig);
             Serde<ProcessedRTCM> processedRTCMSerde = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedRTCM();
             Serde<RsuStationIdKey> keySerde = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey();
             Serde<RtcmMinimumDataEvent> minDataEventSerde = JsonSerdes.RtcmMinimumDataEvent();
             Serde<RtcmBroadcastRateEvent> broadcastRateEventSerde = JsonSerdes.RtcmBroadcastRateEvent()) {

            var inputTopic = driver.createInputTopic(inputTopicName,
                    keySerde.serializer(), processedRTCMSerde.serializer());

            var broadcastRateTopic = driver.createOutputTopic(broadcastRateTopicName,
                    keySerde.deserializer(),  broadcastRateEventSerde.deserializer());

            var minimumDataTopic = driver.createOutputTopic(minimumDataTopicName,
                    keySerde.deserializer(), minDataEventSerde.deserializer());

            final RsuStationIdKey key = new RsuStationIdKey();
            key.setRsuId(rsuId);
            key.setStationId(stationId);

            // Send at .5 Hz (slow)
            final int slowPeriodMillis = 2000;
            final int totalTimeSeconds = 13;
            List<Instant> instants = TopologyTestUtils.getInstants(startTime, slowPeriodMillis, totalTimeSeconds);
            for (var currentInstant : instants) {
                var map = createRtcm(currentInstant);
                inputTopic.pipeInput(key, map, currentInstant);
            }

            var minDataList = minimumDataTopic.readKeyValuesToList();
            assertThat("Should be > 1 min data events", minDataList, hasSize(greaterThan(1)));
            for (var entry : minDataList) {
                var resultKey = entry.key;
                assertThat("min data key rsuId", resultKey.getRsuId(), equalTo(rsuId));
                assertThat("min data key stationId", resultKey.getStationId(), equalTo(stationId));
                var result = entry.value;
                assertThat("min data event rsuId", result.getSource(), equalTo(source));
                assertThat("min data stationId", result.getStationId(), equalTo(stationId));
                assertThat("min data missingDataElements size", result.getMissingDataElements(), hasSize(1));
                var msg = result.getMissingDataElements().getFirst();
                assertThat("min data validation message match", msg, startsWith(validationMsg));
            }

            var broadcastRateList = broadcastRateTopic.readKeyValuesToList();
            assertThat("Should be 1 broadcast rate event", broadcastRateList, hasSize(1));
            var broadcastRate = broadcastRateList.getFirst();
            var bcKey =  broadcastRate.key;
            assertThat("broadcast rate key rsuId", bcKey.getRsuId(), equalTo(rsuId));
            assertThat("broadcast rate key stationId", bcKey.getStationId(), equalTo(stationId));
            var bcValue = broadcastRate.value;
            assertThat("broadcast rate device id", bcValue.getSource(), equalTo(source));
            assertThat("broadcast rate stationId", bcValue.getIntersectionID(), equalTo(stationId));
            assertThat("broadcast rate topic name", bcValue.getTopicName(), equalTo(inputTopicName));
            assertThat("broadcast rate number of messages", bcValue.getNumberOfMessages(), equalTo(50));
            assertThat("broadcast rate time period null", bcValue.getTimePeriod(), notNullValue());
            assertThat("broadcast rate time period", bcValue.getTimePeriod().periodMillis(), equalTo(10000L));

        }

    }

    private Topology createTopology() {
        var parameters = getParameters();
        var rtcmValidationTopology = new RtcmValidationTopology();
        rtcmValidationTopology.setParameters(parameters);
        return rtcmValidationTopology.buildTopology();
    }

    private Properties createStreamsConfig() {
        var streamsConfig = new Properties();
        streamsConfig.setProperty(
                StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                TimestampExtractorForBroadcastRate.class.getName());
        return streamsConfig;
    }

    private RtcmValidationParameters getParameters() {
        var parameters = new RtcmValidationParameters();
        parameters.setInputTopicName(inputTopicName);
        parameters.setBroadcastRateTopicName(broadcastRateTopicName);
        parameters.setMinimumDataTopicName(minimumDataTopicName);
        parameters.setRollingPeriodSeconds(rollingPeriod);
        parameters.setOutputIntervalSeconds(outputInterval);
        parameters.setGracePeriodMilliseconds(gracePeriod);
        parameters.setLowerBound(lowerBound);
        parameters.setUpperBound(upperBound);
        parameters.setDebug(debug);
        return parameters;
    }

    private ProcessedRTCM createRtcm(Instant timestamp) {
        var geometry = new Point(-105.0, 40.0);
        var properties = new RTCMProperties();
        properties.setOdeReceivedAt(timestamp.atZone(ZoneOffset.UTC));
        properties.setCti4501Conformant(false);
        properties.setStationId(stationId);
        var valMsgList = new ArrayList<ProcessedValidationMessage>();
        var msg = new ProcessedValidationMessage();
        msg.setMessage(validationMsg);
        valMsgList.add(msg);
        properties.setValidationMessages(valMsgList);
        return new ProcessedRTCM(geometry, properties);
    }
}
