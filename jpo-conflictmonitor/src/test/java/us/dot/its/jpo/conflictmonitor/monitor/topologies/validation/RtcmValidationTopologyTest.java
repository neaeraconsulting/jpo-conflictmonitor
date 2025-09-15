package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.rtcm.RtcmValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.spat.SpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.timestamp_delta.SpatTimestampDeltaTopology;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;
import us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes;

import java.time.Instant;
import java.util.Properties;

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

    final int lowerBound = 90;
    final int upperBound = 110;
    final boolean debug = true;

    final String validationMsg = "Validation Message";


    final String rsuId = "127.0.0.1";
    final int stationId = 1001;
    final String source = "{ rsuId='127.0.0.1', intersectionId='11111', region='10'}";
    final int intersectionId = 11111;
    final int region = 10;

    @Test
    public void testRtcmValidationTopology() {

        Properties streamsConfig = createStreamsConfig();
        Topology topology = createTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig);
             Serde<ProcessedRTCM> processedRTCMSerde = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedRTCM();
             Serde<RsuStationIdKey> keySerde = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey()) {

            var inputTopic = driver.createInputTopic(inputTopicName,
                    keySerde.serializer(), processedRTCMSerde.serializer());

            final RsuStationIdKey key = new RsuStationIdKey();
            key.setRsuId(rsuId);
            key.setStationId(stationId);



        } finally {

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
}
