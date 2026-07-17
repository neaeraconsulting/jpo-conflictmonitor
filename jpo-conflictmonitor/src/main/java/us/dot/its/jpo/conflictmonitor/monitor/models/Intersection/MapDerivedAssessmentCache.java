package us.dot.its.jpo.conflictmonitor.monitor.models.Intersection;

import us.dot.its.jpo.conflictmonitor.monitor.models.RegulatorIntersectionId;
import us.dot.its.jpo.conflictmonitor.monitor.models.concurrent_permissive.ConnectedLanesPair;
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

/**
 * Cached MAP-derived data for Map/SPaT assessment: signal groups, revocable lanes,
 * and geometrically conflicting connection pairs. Rebuilt only when MAP revision/content changes.
 */
public class MapDerivedAssessmentCache {

    private final long contentKey;
    private final int intersectionId;
    private final int roadRegulatorId;
    private final Set<Integer> mapSignalGroups;
    private final Set<Integer> revocableLaneIds;
    private final List<ConflictingConnectionPair> conflictPairs;
    private final RegulatorIntersectionId mapRegulatorIntersectionId;

    public MapDerivedAssessmentCache(
            long contentKey,
            int intersectionId,
            int roadRegulatorId,
            Set<Integer> mapSignalGroups,
            Set<Integer> revocableLaneIds,
            List<ConflictingConnectionPair> conflictPairs,
            RegulatorIntersectionId mapRegulatorIntersectionId) {
        this.contentKey = contentKey;
        this.intersectionId = intersectionId;
        this.roadRegulatorId = roadRegulatorId;
        this.mapSignalGroups = Collections.unmodifiableSet(mapSignalGroups);
        this.revocableLaneIds = Collections.unmodifiableSet(revocableLaneIds);
        this.conflictPairs = Collections.unmodifiableList(conflictPairs);
        this.mapRegulatorIntersectionId = mapRegulatorIntersectionId;
    }

    public long getContentKey() {
        return contentKey;
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

        public MapDerivedAssessmentCache getOrBuild(
                String cacheKey,
                ProcessedMap<LineString> map,
                Set<ConnectedLanesPair> allowConcurrentPermissiveSet) {
            long contentKey = computeContentKey(map);
            MapDerivedAssessmentCache existing = caches.get(cacheKey);
            if (existing != null && existing.contentKey == contentKey) {
                return existing;
            }
            MapDerivedAssessmentCache built = build(contentKey, map, allowConcurrentPermissiveSet);
            caches.put(cacheKey, built);
            return built;
        }

        public void clear() {
            caches.clear();
        }
    }

    public static long computeContentKey(ProcessedMap<LineString> map) {
        if (map == null || map.getProperties() == null) {
            return 0L;
        }
        Integer revision = map.getProperties().getRevision();
        Integer msgIssueRevision = map.getProperties().getMsgIssueRevision();
        Integer intersectionId = map.getProperties().getIntersectionId();
        Integer region = map.getProperties().getRegion();
        long key = Objects.hash(revision, msgIssueRevision, intersectionId, region);

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
                intersectionId,
                roadRegulatorId,
                mapSignalGroups,
                revocableLaneIds,
                conflictPairs,
                mapId);
    }
}
