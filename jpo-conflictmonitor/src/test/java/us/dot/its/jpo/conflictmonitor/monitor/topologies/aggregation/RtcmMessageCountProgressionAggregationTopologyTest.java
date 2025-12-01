package us.dot.its.jpo.conflictmonitor.monitor.topologies.aggregation;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.rtcm_message_count_progression.RtcmMessageCountProgressionAggregationKey;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.RtcmMessageCountProgressionEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.RtcmMessageCountProgressionEventAggregation;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;

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

    @Test
    public void testTopology() {
        assertThat(true, equalTo(false));
    }

    @Override
    String outputTopicName() {
        return "";
    }

    @Override
    Serde<RsuStationIdKey> eventKeySerde() {
        return null;
    }

    @Override
    Serde<RtcmMessageCountProgressionEvent> eventSerde() {
        return null;
    }

    @Override
    Serde<RtcmMessageCountProgressionAggregationKey> aggKeySerde() {
        return null;
    }

    @Override
    Serde<RtcmMessageCountProgressionEventAggregation> aggEventSerde() {
        return null;
    }

    @Override
    RsuStationIdKey createKey() {
        return null;
    }

    @Override
    RtcmMessageCountProgressionEvent createEvent() {
        return null;
    }

    @Override
    RtcmMessageCountProgressionAggregationTopology createTopology() {
        return null;
    }

    @Override
    KStream<RtcmMessageCountProgressionAggregationKey, RtcmMessageCountProgressionEvent> selectAggKey(KStream<RsuStationIdKey, RtcmMessageCountProgressionEvent> instream) {
        return null;
    }
}
