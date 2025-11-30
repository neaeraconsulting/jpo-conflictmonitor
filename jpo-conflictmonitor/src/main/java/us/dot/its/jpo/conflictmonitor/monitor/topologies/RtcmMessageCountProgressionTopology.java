package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.BaseStreamsTopology;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.rtcm_message_count_progression.RtcmMessageCountProgressionAggregationAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.rtcm_message_count_progression.RtcmMessageCountProgressionAggregationKey;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.rtcm_message_count_progression.RtcmMessageCountProgressionAggregationStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression.RtcmMessageCountProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression.RtcmMessageCountProgressionStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.processors.RtcmMessageCountProgressionProcessor;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIdPartitioner;

import java.time.Duration;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression.RtcmMessageCountProgressionConstants.DEFAULT_RTCM_MESSAGE_COUNT_PROGRESSION_ALGORITHM;

@Component(DEFAULT_RTCM_MESSAGE_COUNT_PROGRESSION_ALGORITHM)
@Slf4j
public class RtcmMessageCountProgressionTopology
    extends BaseStreamsTopology<RtcmMessageCountProgressionParameters>
    implements RtcmMessageCountProgressionStreamsAlgorithm {

    @Override
    protected Logger getLogger() {
        return log;
    }

    RtcmMessageCountProgressionAggregationStreamsAlgorithm aggregationAlgorithm;

    @Override
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        final String processedRtcmStateStore = parameters.getProcessedRtcmStateStoreName();
        final String latestRtcmStateStore = parameters.getLatestRtcmStateStoreName();
        final Duration retentionTime = Duration.ofMillis(parameters.getBufferTimeMs());

        builder.addStateStore(
                Stores.versionedKeyValueStoreBuilder(
                        Stores.persistentVersionedKeyValueStore(processedRtcmStateStore, retentionTime),
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey(),
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedRTCM()));

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(latestRtcmStateStore),
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey(),
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedRTCM()));

        var eventStream = builder
                .stream(parameters.getRtcmInputTopicName(),
                        Consumed.with(
                                us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey(),
                                us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedRTCM()))
                .process(() -> new RtcmMessageCountProgressionProcessor(parameters), processedRtcmStateStore, latestRtcmStateStore);

        if (parameters.isAggregateEvents()) {
            // Aggregate events
            // Select new key with aggregation fields
            var aggKeyStream = eventStream.selectKey((key, value) -> {
                var aggKey = new RtcmMessageCountProgressionAggregationKey();
                aggKey.setRsuId(key.getRsuId());
                aggKey.setStationId(key.getStationId());
                aggKey.setChange(value.getChange());
                return aggKey;
            })
            .repartition(
                   Repartitioned.with(
                           JsonSerdes.RtcmMessageCountProgressionAggregationKey(),
                           JsonSerdes.RtcmMessageCountProgressionEvent()
                   )
                           .withStreamPartitioner(new RsuIdPartitioner<>())
            );
            aggregationAlgorithm.buildTopology(builder, aggKeyStream);
        } else {
            // Don't aggregate
            eventStream.to(parameters.getRtcmMessageCountProgressionOutputTopicName(),
                    Produced.with(
                            us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuStationIdKey(),
                            JsonSerdes.RtcmMessageCountProgressionEvent()
                    ).withStreamPartitioner(new RsuIdPartitioner<>()));
        }

        return builder.build();
    }

    @Override
    public void setAggregationAlgorithm(RtcmMessageCountProgressionAggregationAlgorithm aggregationAlgorithm) {
// Enforce the algorithm being a Streams algorithm
        if (aggregationAlgorithm instanceof RtcmMessageCountProgressionAggregationStreamsAlgorithm streamsAlgorithm) {
            this.aggregationAlgorithm = streamsAlgorithm;
        } else {
            throw new IllegalArgumentException("Aggregation algorithm must be a Streams algorithm");
        }
    }
}
