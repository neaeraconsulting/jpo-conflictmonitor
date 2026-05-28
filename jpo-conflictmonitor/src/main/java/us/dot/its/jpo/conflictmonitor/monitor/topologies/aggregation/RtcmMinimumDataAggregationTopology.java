package us.dot.its.jpo.conflictmonitor.monitor.topologies.aggregation;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.validation.rtcm.RtcmMinimumDataAggregationStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.RtcmMinimumDataEvent;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.minimum_data.RtcmMinimumDataEventAggregation;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuStationIdKey;


import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.AggregationConstants.DEFAULT_RTCM_MINIMUM_DATA_AGGREGATION_ALGORITHM;

@Component(DEFAULT_RTCM_MINIMUM_DATA_AGGREGATION_ALGORITHM)
@Slf4j
public class RtcmMinimumDataAggregationTopology
    extends
        BaseAggregationTopology<
                RsuStationIdKey,
                RtcmMinimumDataEvent,
                RtcmMinimumDataEventAggregation>
    implements
        RtcmMinimumDataAggregationStreamsAlgorithm {
    @Override
    protected Logger getLogger() {
        return log;
    }

    @Override
    public RtcmMinimumDataEventAggregation constructEventAggregation(RtcmMinimumDataEvent event) {
        var aggEvent = new  RtcmMinimumDataEventAggregation();
        aggEvent.setSource(event.getSource());
        aggEvent.setStationId(event.getStationId());
        return aggEvent;
    }

    @Override
    public String eventAggregationType() {
        return new RtcmMinimumDataEventAggregation().getEventType();
    }

    @Override
    public Class<RsuStationIdKey> keyClass() {
        return RsuStationIdKey.class;
    }

    @Override
    public Serde<RsuStationIdKey> keySerde() {
        return us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey();
    }

    @Override
    public Serde<RtcmMinimumDataEvent> eventSerde() {
        return JsonSerdes.RtcmMinimumDataEvent();
    }

    @Override
    public Serde<RtcmMinimumDataEventAggregation> eventAggregationSerde() {
        return JsonSerdes.RtcmMinimumDataEventAggregation();
    }

    // No partitioner
    @Override
    public StreamPartitioner<RsuStationIdKey, RtcmMinimumDataEventAggregation> eventAggregationPartitioner() {
        return null;
    }
}
