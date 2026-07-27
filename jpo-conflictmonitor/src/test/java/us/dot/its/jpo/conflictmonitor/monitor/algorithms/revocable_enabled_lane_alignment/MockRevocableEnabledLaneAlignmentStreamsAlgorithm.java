package us.dot.its.jpo.conflictmonitor.monitor.algorithms.revocable_enabled_lane_alignment;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.revocable_enabled_lane_alignment.RevocableEnabledLaneAlignmentAggregationAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.Intersection.MapDerivedAssessmentCache;
import us.dot.its.jpo.conflictmonitor.monitor.models.SpatMap;
import us.dot.its.jpo.conflictmonitor.monitor.models.concurrent_permissive.ConnectedLanesPair;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;

import java.util.Set;

/**
 * Mock of Revocable Lanes subtopology that does nothing for testing other parts of the Map/Spat Message Alignment
 * topology.
 */
public class MockRevocableEnabledLaneAlignmentStreamsAlgorithm implements RevocableEnabledLaneAlignmentStreamsAlgorithm {

    @Override
    public void setAggregationAlgorithm(RevocableEnabledLaneAlignmentAggregationAlgorithm aggregationAlgorithm) {
        // Do nothing
    }

    @Override
    public void setParameters(RevocableEnabledLaneAlignmentParameters revocableEnabledLaneAlignmentParameters) {
        // Do nothing
    }

    @Override
    public RevocableEnabledLaneAlignmentParameters getParameters() {
        return new RevocableEnabledLaneAlignmentParameters();
    }

    @Override
    public void buildTopology(StreamsBuilder builder, KStream<RsuIntersectionKey, SpatMap> spatMapStream) {
        // Do nothing
    }

    @Override
    public void setAssessmentCacheManager(
            MapDerivedAssessmentCache.Manager assessmentCacheManager,
            Set<ConnectedLanesPair> allowConcurrentPermissiveSet) {
        // Do nothing
    }

}
