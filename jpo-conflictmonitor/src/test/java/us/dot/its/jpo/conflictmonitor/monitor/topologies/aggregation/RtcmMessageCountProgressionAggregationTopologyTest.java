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
import static org.hamcrest.Matchers.*;

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
    private final List<String> change = List.of("x", "y", "z");

    @Test
    public void testTopology() {
        var resultList = runTestTopology();
        assertThat("Should have produced 1 aggregated event", resultList, hasSize(1));
        var result = resultList.getFirst();
        log.info("Agg result: {}", result);
        var resultKey = result.key;
        assertThat(resultKey.getRsuId(), equalTo(rsuId));
        assertThat(resultKey.getStationId(), equalTo(stationId));
        assertThat(resultKey.getChange(), equalTo(change));
        var resultValue = result.value;
        assertThat(resultValue.getNumberOfEvents(), equalTo(numberOfEvents));
        assertThat(resultValue.getChange(), equalTo(change));
        assertThat(resultValue.getStationId(), equalTo(stationId));
        var period = resultValue.getTimePeriod();
        assertThat(period, notNullValue());
        assertThat(period.getBeginTimestamp(), equalTo(initialWallClock.toEpochMilli()));
        assertThat(period.getEndTimestamp(), equalTo(initialWallClock.toEpochMilli() + intervalSeconds*1000));

    }

    @Override
    String outputTopicName() {
        return "topic.CmRtcmMessageCountProgressionEventAggregation";
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
        event.setStationId(stationId);
        event.setIntersectionID(-1);
        event.setRoadRegulatorID(-1);
        event.setMessageCountA(1);
        event.setMessageCountB(1);
        event.setTimestampA(initialWallClock.plusMillis(1).toEpochMilli());
        event.setTimestampB(initialWallClock.plusMillis(10).toEpochMilli());
        event.setChange(change);
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
            aggKey.setChange(change);
            return aggKey;
        });
    }
}
