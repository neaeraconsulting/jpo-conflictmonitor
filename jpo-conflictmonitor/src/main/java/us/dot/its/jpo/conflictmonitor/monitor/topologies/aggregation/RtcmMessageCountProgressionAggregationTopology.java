package us.dot.its.jpo.conflictmonitor.monitor.topologies.aggregation;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.rtcm_message_count_progression.RtcmMessageCountProgressionAggregationKey;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.rtcm_message_count_progression.RtcmMessageCountProgressionAggregationStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.RtcmMessageCountProgressionEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.RtcmMessageCountProgressionEventAggregation;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIdPartitioner;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.AggregationConstants.DEFAULT_RTCM_MESSAGE_COUNT_PROGRESSION_AGGREGATION_ALGORITHM;

@Component(DEFAULT_RTCM_MESSAGE_COUNT_PROGRESSION_AGGREGATION_ALGORITHM)
@Slf4j
public class RtcmMessageCountProgressionAggregationTopology
    extends
        BaseAggregationTopology<
            RtcmMessageCountProgressionAggregationKey,
            RtcmMessageCountProgressionEvent,
            RtcmMessageCountProgressionEventAggregation>
    implements
        RtcmMessageCountProgressionAggregationStreamsAlgorithm {

    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    public RtcmMessageCountProgressionEventAggregation constructEventAggregation(RtcmMessageCountProgressionEvent event) {
        var aggEvent = new RtcmMessageCountProgressionEventAggregation();
        aggEvent.setSource(event.getSource());
        aggEvent.setStationId(event.getStationId());
        aggEvent.setChange(event.getChange());
        return aggEvent;
    }

    @Override
    public String eventAggregationType() {
        return new RtcmMessageCountProgressionEventAggregation().getEventType();
    }

    @Override
    public Class<RtcmMessageCountProgressionAggregationKey> keyClass() {
        return RtcmMessageCountProgressionAggregationKey.class;
    }

    @Override
    public Serde<RtcmMessageCountProgressionAggregationKey> keySerde() {
        return JsonSerdes.RtcmMessageCountProgressionAggregationKey();
    }

    @Override
    public Serde<RtcmMessageCountProgressionEvent> eventSerde() {
        return JsonSerdes.RtcmMessageCountProgressionEvent();
    }

    @Override
    public Serde<RtcmMessageCountProgressionEventAggregation> eventAggregationSerde() {
        return JsonSerdes.RtcmMessageCountProgressionEventAggregation();
    }

    @Override
    public StreamPartitioner<RtcmMessageCountProgressionAggregationKey, RtcmMessageCountProgressionEventAggregation> eventAggregationPartitioner() {
        return new RsuIdPartitioner<>();
    }
}
