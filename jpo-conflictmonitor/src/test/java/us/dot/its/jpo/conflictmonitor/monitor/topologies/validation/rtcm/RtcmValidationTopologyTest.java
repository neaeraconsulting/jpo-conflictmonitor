package us.dot.its.jpo.conflictmonitor.monitor.topologies.validation.rtcm;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.RtcmMinimumDataEvent;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.rtcm.ProcessedRTCM;

import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@Slf4j
public class RtcmValidationTopologyTest extends BaseRtcmValidationTopologyTest {

    @Test
    public void testRtcmValidationTopologyMinimumDataEvents() {
        Properties streamsConfig = createStreamsConfig();
        Topology topology = createTopology();

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, streamsConfig);
             Serde<ProcessedRTCM> processedRTCMSerde = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedRTCM();
             Serde<RsuStationIdKey> keySerde = us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey();
             Serde<RtcmMinimumDataEvent> minDataEventSerde = JsonSerdes.RtcmMinimumDataEvent()) {

            var inputTopic = driver.createInputTopic(inputTopicName,
                    keySerde.serializer(), processedRTCMSerde.serializer());

            var minimumDataTopic = driver.createOutputTopic(minimumDataTopicName,
                    keySerde.deserializer(), minDataEventSerde.deserializer());

            final RsuStationIdKey key = new RsuStationIdKey();
            key.setRsuId(rsuId);
            key.setStationId(stationId);

            var rtcm1 = createRtcm(startTime);
            var instant2 = startTime.plusMillis(100);
            var rtcm2 = createRtcm(instant2);
            inputTopic.pipeInput(key, rtcm1, startTime);
            inputTopic.pipeInput(key, rtcm2, instant2);

            var minDataList = minimumDataTopic.readKeyValuesToList();
            assertThat(minDataList, hasSize(equalTo(2)));
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

        }
    }


}
