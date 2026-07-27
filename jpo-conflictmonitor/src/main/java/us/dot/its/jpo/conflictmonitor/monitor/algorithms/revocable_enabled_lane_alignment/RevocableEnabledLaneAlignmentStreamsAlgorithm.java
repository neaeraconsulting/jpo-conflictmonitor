package us.dot.its.jpo.conflictmonitor.monitor.algorithms.revocable_enabled_lane_alignment;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import us.dot.its.jpo.conflictmonitor.monitor.models.Intersection.MapDerivedAssessmentCache;
import us.dot.its.jpo.conflictmonitor.monitor.models.SpatMap;
import us.dot.its.jpo.conflictmonitor.monitor.models.concurrent_permissive.ConnectedLanesPair;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;

import java.util.Set;

public interface RevocableEnabledLaneAlignmentStreamsAlgorithm extends RevocableEnabledLaneAlignmentAlgorithm {

    void buildTopology(StreamsBuilder builder, KStream<RsuIntersectionKey, SpatMap> spatMapStream);

    /**
     * Shares MapSpat's MAP-derived cache so revocable lane attributes are not rebuilt every SPaT.
     */
    void setAssessmentCacheManager(
            MapDerivedAssessmentCache.Manager assessmentCacheManager,
            Set<ConnectedLanesPair> allowConcurrentPermissiveSet);

}
