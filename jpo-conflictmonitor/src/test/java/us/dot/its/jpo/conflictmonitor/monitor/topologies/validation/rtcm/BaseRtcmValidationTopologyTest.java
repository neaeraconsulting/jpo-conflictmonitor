package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.rtcm;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.validation.rtcm.RtcmValidationParameters;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.RtcmValidationTopology;
import us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.TimestampExtractorForBroadcastRate;
import us.dot.its.jpo.geojsonconverter.pojos.ProcessedValidationMessage;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.Point;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.RTCMProperties;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

public abstract class BaseRtcmValidationTopologyTest {

    protected final String inputTopicName = "topic.ProcessedRtcm";
    protected final String broadcastRateTopicName = "topic.CmRtcmBroadcastRateEvents";
    protected final String minimumDataTopicName = "topic.CmRtcmMinimumDataEvents";

    // Use a tumbling window for test (rolling period = output interval)
    // just to make it easier to design the test.
    protected final int rollingPeriod = 10;
    protected final int outputInterval = 10;
    protected final int gracePeriod = 100;

    // Start time on 10-second window boundary
    protected final Instant startTime = Instant.ofEpochMilli(1674356320000L);

    protected final int lowerBound = 9;
    protected final int upperBound = 11;
    protected final int msmLowerBound = 9;
    protected final int msmUpperBound = 110;
    protected final boolean debug = true;

    protected final String validationMsg = "Validation Message";

    protected final String rsuId = "127.0.0.1";
    protected final int stationId = 1001;
    protected final String source = rsuId;

    protected Topology createTopology() {
        var parameters = getParameters();
        var rtcmValidationTopology = new RtcmValidationTopology();
        rtcmValidationTopology.setParameters(parameters);
        return rtcmValidationTopology.buildTopology();
    }

    protected Properties createStreamsConfig() {
        var streamsConfig = new Properties();
        streamsConfig.setProperty(
                StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                TimestampExtractorForBroadcastRate.class.getName());
        return streamsConfig;
    }

    protected RtcmValidationParameters getParameters() {
        var parameters = new RtcmValidationParameters();
        parameters.setInputTopicName(inputTopicName);
        parameters.setBroadcastRateTopicName(broadcastRateTopicName);
        parameters.setMinimumDataTopicName(minimumDataTopicName);
        parameters.setRollingPeriodSeconds(rollingPeriod);
        parameters.setOutputIntervalSeconds(outputInterval);
        parameters.setGracePeriodMilliseconds(gracePeriod);
        parameters.setLowerBound(lowerBound);
        parameters.setUpperBound(upperBound);
        parameters.setMsmLowerBound(msmLowerBound);
        parameters.setMsmUpperBound(msmUpperBound);
        parameters.setDebug(debug);
        return parameters;
    }

    /**
     * Create a sample ProcessedRTCM
     * @param timestamp the timestamp
     * @param includeMsm true to include MSM4 message types (eg 1074), false to
     * include basic message types (1005/1006, 1013, 1033).
     * @return the test Processed RTCM
     */
    protected ProcessedRTCM createRtcm(Instant timestamp, boolean includeMsm) {
        var geometry = new Point(-105.0, 40.0);
        var properties = new RTCMProperties();
        properties.setOdeReceivedAt(timestamp.atZone(ZoneOffset.UTC));
        properties.setCti4501Conformant(false);
        properties.setStationId(stationId);
        if (includeMsm) {
            properties.setMessageTypes(Set.of(1074, 1084, 1124));
        } else {
            properties.setMessageTypes(Set.of(1004, 1005, 1012));
        }
        var valMsgList = new ArrayList<ProcessedValidationMessage>();
        var msg = new ProcessedValidationMessage();
        msg.setMessage(validationMsg);
        valMsgList.add(msg);
        properties.setValidationMessages(valMsgList);
        return new ProcessedRTCM(geometry, properties);
    }
}
