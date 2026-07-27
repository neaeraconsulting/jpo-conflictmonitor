package us.dot.its.jpo.conflictmonitor.monitor.models.Intersection;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import us.dot.its.jpo.conflictmonitor.monitor.models.RegulatorIntersectionId;
import us.dot.its.jpo.conflictmonitor.monitor.models.concurrent_permissive.ConnectedLanesPair;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.revocable_enabled_lane_alignment.LaneTypeAttributesMap;
import us.dot.its.jpo.conflictmonitor.monitor.utils.ProcessedMapUtils;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.LineString;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.connectinglanes.ConnectingLanesFeature;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cached MAP-derived data for Map/SPaT assessment: signal groups, revocable lanes,
 * and geometrically conflicting connection pairs. Rebuilt only when MAP revision/content changes.
 */
public class MapDerivedAssessmentCache {

    private final long contentKey;
    /** Cheap fingerprint from MAP revision fields; used to skip full content-key walks on hits. */
    private final long revisionFingerprint;
    private final int intersectionId;
    private final int roadRegulatorId;
    private final Set<Integer> mapSignalGroups;
    private final Set<Integer> revocableLaneIds;
    private final LaneTypeAttributesMap laneTypeAttributes;
    private final List<ConflictingConnectionPair> conflictPairs;
    private final RegulatorIntersectionId mapRegulatorIntersectionId;

    public MapDerivedAssessmentCache(
            long contentKey,
            long revisionFingerprint,
            int intersectionId,
            int roadRegulatorId,
            Set<Integer> mapSignalGroups,
            Set<Integer> revocableLaneIds,
            LaneTypeAttributesMap laneTypeAttributes,
            List<ConflictingConnectionPair> conflictPairs,
            RegulatorIntersectionId mapRegulatorIntersectionId) {
        this.contentKey = contentKey;
        this.revisionFingerprint = revisionFingerprint;
        this.intersectionId = intersectionId;
        this.roadRegulatorId = roadRegulatorId;
        this.mapSignalGroups = Collections.unmodifiableSet(mapSignalGroups);
        this.revocableLaneIds = Collections.unmodifiableSet(revocableLaneIds);
        this.laneTypeAttributes = laneTypeAttributes != null ? laneTypeAttributes : new LaneTypeAttributesMap();
        this.conflictPairs = Collections.unmodifiableList(conflictPairs);
        this.mapRegulatorIntersectionId = mapRegulatorIntersectionId;
    }

    public long getContentKey() {
        return contentKey;
    }

    public long getRevisionFingerprint() {
        return revisionFingerprint;
    }

    public int getIntersectionId() {
        return intersectionId;
    }

    public int getRoadRegulatorId() {
        return roadRegulatorId;
    }

    public Set<Integer> getMapSignalGroups() {
        return mapSignalGroups;
    }

    public Set<Integer> getRevocableLaneIds() {
        return revocableLaneIds;
    }

    public LaneTypeAttributesMap getLaneTypeAttributes() {
        return laneTypeAttributes;
    }

    public List<ConflictingConnectionPair> getConflictPairs() {
        return conflictPairs;
    }

    public RegulatorIntersectionId getMapRegulatorIntersectionId() {
        return mapRegulatorIntersectionId;
    }

    /**
     * Process-local cache keyed by intersection key string.
     */
    public static final class Manager {
        private final ConcurrentHashMap<String, MapDerivedAssessmentCache> caches = new ConcurrentHashMap<>();
        private final AtomicLong hits = new AtomicLong();
        private final AtomicLong misses = new AtomicLong();
        private Counter hitCounter;
        private Counter missCounter;

        public void bindMetrics(MeterRegistry meterRegistry) {
            if (meterRegistry == null) {
                return;
            }
            // register() is idempotent for the same name+tags; safe if topology rebuilds
            hitCounter = Counter.builder("cm.map_spat.cache")
                    .description("MAP-derived assessment cache lookups")
                    .tag("result", "hit")
                    .register(meterRegistry);
            missCounter = Counter.builder("cm.map_spat.cache")
                    .description("MAP-derived assessment cache lookups")
                    .tag("result", "miss")
                    .register(meterRegistry);
            Gauge.builder("cm.map_spat.cache.size", caches, ConcurrentHashMap::size)
                    .description("Number of intersections with cached MAP-derived assessment data")
                    .register(meterRegistry);
        }

        public MapDerivedAssessmentCache getOrBuild(
                String cacheKey,
                ProcessedMap<LineString> map,
                Set<ConnectedLanesPair> allowConcurrentPermissiveSet) {
            MapDerivedAssessmentCache existing = caches.get(cacheKey);
            if (existing != null) {
                // Fast path: revision fingerprint match → skip connecting-lane content walk
                long fingerprint = computeRevisionFingerprint(map);
                if (existing.revisionFingerprint == fingerprint) {
                    hits.incrementAndGet();
                    if (hitCounter != null) {
                        hitCounter.increment();
                    }
                    return existing;
                }
            }

            long contentKey = computeContentKey(map);
            if (existing != null && existing.contentKey == contentKey) {
                hits.incrementAndGet();
                if (hitCounter != null) {
                    hitCounter.increment();
                }
                return existing;
            }

            misses.incrementAndGet();
            if (missCounter != null) {
                missCounter.increment();
            }
            MapDerivedAssessmentCache built = build(contentKey, map, allowConcurrentPermissiveSet);
            caches.put(cacheKey, built);
            return built;
        }

        public void clear() {
            caches.clear();
            hits.set(0);
            misses.set(0);
        }

        public long getHits() {
            return hits.get();
        }

        public long getMisses() {
            return misses.get();
        }
    }

    public static long computeRevisionFingerprint(ProcessedMap<LineString> map) {
        if (map == null || map.getProperties() == null) {
            return 0L;
        }
        return Objects.hash(
                map.getProperties().getRevision(),
                map.getProperties().getMsgIssueRevision(),
                map.getProperties().getIntersectionId(),
                map.getProperties().getRegion());
    }

    public static long computeContentKey(ProcessedMap<LineString> map) {
        if (map == null || map.getProperties() == null) {
            return 0L;
        }
        long key = computeRevisionFingerprint(map);

        if (map.getConnectingLanesFeatureCollection() != null
                && map.getConnectingLanesFeatureCollection().getFeatures() != null) {
            for (ConnectingLanesFeature<?> feature : map.getConnectingLanesFeatureCollection().getFeatures()) {
                if (feature.getProperties() == null) {
                    continue;
                }
                key = 31 * key + Objects.hash(
                        feature.getProperties().getIngressLaneId(),
                        feature.getProperties().getEgressLaneId(),
                        feature.getProperties().getSignalGroupId());
            }
        }

        for (Integer laneId : ProcessedMapUtils.getRevocableLanes(map)) {
            key = 31 * key + Objects.hash(laneId);
        }
        return key;
    }

    public static MapDerivedAssessmentCache build(
            long contentKey,
            ProcessedMap<LineString> map,
            Set<ConnectedLanesPair> allowConcurrentPermissiveSet) {

        long revisionFingerprint = computeRevisionFingerprint(map);

        Set<Integer> mapSignalGroups = new HashSet<>();
        if (map != null
                && map.getConnectingLanesFeatureCollection() != null
                && map.getConnectingLanesFeatureCollection().getFeatures() != null) {
            for (ConnectingLanesFeature<?> feature : map.getConnectingLanesFeatureCollection().getFeatures()) {
                if (feature.getProperties() != null && feature.getProperties().getSignalGroupId() != null) {
                    mapSignalGroups.add(feature.getProperties().getSignalGroupId());
                }
            }
        }

        RegulatorIntersectionId mapId = new RegulatorIntersectionId();
        int intersectionId = -1;
        int roadRegulatorId = -1;
        if (map != null && map.getProperties() != null) {
            if (map.getProperties().getIntersectionId() != null) {
                intersectionId = map.getProperties().getIntersectionId();
                mapId.setIntersectionId(intersectionId);
            }
            if (map.getProperties().getRegion() != null) {
                roadRegulatorId = map.getProperties().getRegion();
                mapId.setRoadRegulatorId(roadRegulatorId);
            } else {
                mapId.setRoadRegulatorId(-1);
            }
        }

        Intersection intersection = Intersection.fromProcessedMap(map);
        Set<Integer> revocableLaneIds = intersection.getRevocableLaneIds() != null
                ? new HashSet<>(intersection.getRevocableLaneIds())
                : Set.of();

        LaneTypeAttributesMap laneTypeAttributes = map != null
                ? ProcessedMapUtils.getLaneTypeAttributesMap(map)
                : new LaneTypeAttributesMap();

        List<ConflictingConnectionPair> conflictPairs = new ArrayList<>();
        ArrayList<LaneConnection> connections = intersection.getLaneConnections();
        if (connections != null) {
            for (int i = 0; i < connections.size(); i++) {
                LaneConnection first = connections.get(i);
                if (first.getIngressLane() == null || first.getEgressLane() == null) {
                    continue;
                }
                for (int j = i + 1; j < connections.size(); j++) {
                    LaneConnection second = connections.get(j);
                    if (second.getIngressLane() == null || second.getEgressLane() == null) {
                        continue;
                    }

                    ConnectedLanesPair theseConnectedLanes = new ConnectedLanesPair(
                            intersection.getIntersectionId(),
                            intersection.getRoadRegulatorId(),
                            first.getIngressLane().getId(),
                            first.getEgressLane().getId(),
                            second.getIngressLane().getId(),
                            second.getEgressLane().getId());

                    if (allowConcurrentPermissiveSet != null
                            && allowConcurrentPermissiveSet.contains(theseConnectedLanes)) {
                        continue;
                    }

                    if (first.crosses(second)
                            && first.getIngressLane() != second.getIngressLane()) {
                        conflictPairs.add(new ConflictingConnectionPair(
                                first.getIngressLane().getId(),
                                first.getEgressLane().getId(),
                                first.getSignalGroup(),
                                second.getIngressLane().getId(),
                                second.getEgressLane().getId(),
                                second.getSignalGroup()));
                    }
                }
            }
        }

        return new MapDerivedAssessmentCache(
                contentKey,
                revisionFingerprint,
                intersectionId,
                roadRegulatorId,
                mapSignalGroups,
                revocableLaneIds,
                laneTypeAttributes,
                conflictPairs,
                mapId);
    }
}
