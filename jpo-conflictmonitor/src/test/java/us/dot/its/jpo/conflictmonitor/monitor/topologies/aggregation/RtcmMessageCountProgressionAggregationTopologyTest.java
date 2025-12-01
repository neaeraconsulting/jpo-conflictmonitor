package us.dot.its.jpo.conflictmonitor.monitor.topologies.aggregation;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.rtcm_message_count_progression.RtcmMessageCountProgressionAggregationKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.RtcmMessageCountProgressionEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.RtcmMessageCountProgressionEventAggregation;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Slf4j
public class RtcmMessageCountProgressionAggregationTopologyTest
    extends
        BaseAggregationTopologyTest<
                RsuStationIdKey,
                RtcmMessageCountProgressionEvent,
                RtcmMessageCountProgressionAggregationKey,
                RtcmMessageCountProgressionEventAggregation,
                RtcmMessageCountProgressionAggregationTopology>{

    private final int stationId = 2432;

    @Test
    public void testTopology() {
        assertThat(true, equalTo(false));
    }

    @Override
    String outputTopicName() {
        return "topic.CmRevocableEnabledLaneAlignmentEventAggregation";
    }

    @Override
    Serde<RsuStationIdKey> eventKeySerde() {
        return us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey();
    }

    @Override
    Serde<RtcmMessageCountProgressionEvent> eventSerde() {
        return JsonSerdes.RtcmMessageCountProgressionEvent();
    }

    @Override
    Serde<RtcmMessageCountProgressionAggregationKey> aggKeySerde() {
        return JsonSerdes.RtcmMessageCountProgressionAggregationKey();
    }

    @Override
    Serde<RtcmMessageCountProgressionEventAggregation> aggEventSerde() {
        return JsonSerdes.RtcmMessageCountProgressionEventAggregation();
    }

    @Override
    RsuStationIdKey createKey() {
        var key = new RsuStationIdKey();
        key.setRsuId(rsuId);
        key.setStationId(stationId);
        return key;
    }

    @Override
    RtcmMessageCountProgressionEvent createEvent() {
        var event = new RtcmMessageCountProgressionEvent();
        event.setSource(rsuId);
        event.setIntersectionID(-1);
        event.setRoadRegulatorID(-1);
        event.setMessageCountA(1);
        event.setMessageCountB(1);
        event.setTimestampA(initialWallClock.plusMillis(1).toEpochMilli());
        event.setTimestampB(initialWallClock.plusMillis(10).toEpochMilli());
        event.setChange(List.of("x", "y"));
        return event;
    }

    @Override
    RtcmMessageCountProgressionAggregationTopology createTopology() {
        return new RtcmMessageCountProgressionAggregationTopology();
    }

    @Override
    KStream<RtcmMessageCountProgressionAggregationKey, RtcmMessageCountProgressionEvent>
    selectAggKey(KStream<RsuStationIdKey, RtcmMessageCountProgressionEvent> instream) {
        return instream.selectKey((key, value) -> {
            var aggKey = new RtcmMessageCountProgressionAggregationKey();
            aggKey.setRsuId(key.getRsuId());
            aggKey.setStationId(key.getStationId());
            aggKey.setChange(new ArrayList<>());
            return aggKey;
        });
    }
}
