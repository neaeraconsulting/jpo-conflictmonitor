package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.BaseStreamsTopology;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.aggregation.map_spat_message_assessment.*;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.map_spat_message_assessment.MapSpatMessageAssessmentParameters;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.map_spat_message_assessment.MapSpatMessageAssessmentStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.revocable_enabled_lane_alignment.RevocableEnabledLaneAlignmentAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.revocable_enabled_lane_alignment.RevocableEnabledLaneAlignmentStreamsAlgorithm;
import us.dot.its.jpo.conflictmonitor.monitor.models.Intersection.AlignmentEmitGate;
import us.dot.its.jpo.conflictmonitor.monitor.models.Intersection.ConflictingConnectionPair;
import us.dot.its.jpo.conflictmonitor.monitor.models.Intersection.MapDerivedAssessmentCache;
import us.dot.its.jpo.conflictmonitor.monitor.models.RegulatorIntersectionId;
import us.dot.its.jpo.conflictmonitor.monitor.models.SpatMap;
import us.dot.its.jpo.conflictmonitor.monitor.models.concurrent_permissive.ConnectedLanesPair;
import us.dot.its.jpo.conflictmonitor.monitor.models.concurrent_permissive.ConnectedLanesPairList;
import us.dot.its.jpo.conflictmonitor.monitor.models.config.ConfigMap;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.*;
import us.dot.its.jpo.conflictmonitor.monitor.models.notifications.*;
import us.dot.its.jpo.conflictmonitor.monitor.models.spat.SpatTimestampExtractor;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.IntersectionIdPartitioner;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.LineString;
import us.dot.its.jpo.geojsonconverter.pojos.geojson.map.ProcessedMap;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementEvent;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;


import java.util.*;

import static us.dot.its.jpo.conflictmonitor.monitor.algorithms.map_spat_message_assessment.MapSpatMessageAssessmentConstants.DEFAULT_MAP_SPAT_MESSAGE_ASSESSMENT_ALGORITHM;

@Slf4j
@Component(DEFAULT_MAP_SPAT_MESSAGE_ASSESSMENT_ALGORITHM)
public class MapSpatMessageAssessmentTopology
        extends BaseStreamsTopology<MapSpatMessageAssessmentParameters>
        implements MapSpatMessageAssessmentStreamsAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(MapSpatMessageAssessmentTopology.class);

    private IntersectionReferenceAlignmentAggregationStreamsAlgorithm intersectionReferenceAlignmentAggregationAlgorithm;
    private SignalGroupAlignmentAggregationStreamsAlgorithm signalGroupAlignmentAggregationAlgorithm;
    private SignalStateConflictAggregationStreamsAlgorithm signalStateConflictAggregationAlgorithm;
    private RevocableEnabledLaneAlignmentStreamsAlgorithm revocableEnabledLaneAlignmentAlgorithm;

    private final MapDerivedAssessmentCache.Manager assessmentCacheManager = new MapDerivedAssessmentCache.Manager();
    private AlignmentEmitGate referenceAlignmentGate;
    private AlignmentEmitGate signalGroupAlignmentGate;

    @Override
    protected Logger getLogger() {
        return logger;
    }

    private ProcessedMovementPhaseState getSpatEventStateBySignalGroup(ProcessedSpat spat, int signalGroup) {
        if (spat == null || spat.getStates() == null) {
            return null;
        }
        for (ProcessedMovementState state : spat.getStates()) {
            if (state.getSignalGroup() == signalGroup) {
                List<ProcessedMovementEvent> movementEvents = state.getStateTimeSpeed();
                if (movementEvents != null && !movementEvents.isEmpty()) {
                    return movementEvents.getFirst().getEventState();
                }
            }
        }
        return null;
    }

//    private String hashLaneConnection(Integer intersectionID, int ingressOne, int ingressTwo, int egressOne, int egressTwo){
//        return intersectionID + "_" + ingressOne + "_" + ingressTwo + "_" + egressOne + "_" + egressTwo;
//    }

    private boolean doStatesConflict(ProcessedMovementPhaseState a, ProcessedMovementPhaseState b) {
        return a.equals(ProcessedMovementPhaseState.PROTECTED_CLEARANCE)
                        && !b.equals(ProcessedMovementPhaseState.STOP_AND_REMAIN)
                ||
                a.equals(ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED)
                        && !b.equals(ProcessedMovementPhaseState.STOP_AND_REMAIN)
                ||
                b.equals(ProcessedMovementPhaseState.PROTECTED_CLEARANCE)
                        && !a.equals(ProcessedMovementPhaseState.STOP_AND_REMAIN)
                ||
                b.equals(ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED)
                        && !a.equals(ProcessedMovementPhaseState.STOP_AND_REMAIN);
    }

    private boolean isRevocableDisabled(
            int ingressId,
            int egressId,
            Set<Integer> revocableLaneIds,
            Set<Integer> enabledLanes) {
        boolean ingressIsRevocable = revocableLaneIds != null && revocableLaneIds.contains(ingressId);
        boolean egressIsRevocable = revocableLaneIds != null && revocableLaneIds.contains(egressId);
        boolean ingressNotEnabled = !enabledLanes.contains(ingressId);
        boolean egressNotEnabled = !enabledLanes.contains(egressId);
        return (ingressIsRevocable && ingressNotEnabled) || (egressIsRevocable && egressNotEnabled);
    }

    private List<KeyValue<String, IntersectionReferenceAlignmentEvent>> evaluateIntersectionReferenceAlignment(
            String rsuKey,
            SpatMap value) {
        ArrayList<KeyValue<String, IntersectionReferenceAlignmentEvent>> events = new ArrayList<>();
        if (value == null || value.getSpat() == null) {
            return events;
        }

        IntersectionReferenceAlignmentEvent event = new IntersectionReferenceAlignmentEvent();
        event.setSource(rsuKey);

        RegulatorIntersectionId mapId = new RegulatorIntersectionId();
        RegulatorIntersectionId spatId = new RegulatorIntersectionId();

        if (value.getMap() != null && value.getMap().getProperties() != null) {
            ProcessedMap<?> map = value.getMap();
            mapId.setIntersectionId(map.getProperties().getIntersectionId());
            mapId.setRoadRegulatorId(map.getProperties().getRegion());

            if (map.getProperties().getIntersectionId() != null) {
                event.setIntersectionID(map.getProperties().getIntersectionId());
            }
            if (map.getProperties().getRegion() != null) {
                event.setRoadRegulatorID(map.getProperties().getRegion());
            } else {
                event.setRoadRegulatorID(-1);
            }
        }

        ProcessedSpat spat = value.getSpat();
        long nowMs = SpatTimestampExtractor.getSpatTimestamp(spat);
        event.setTimestamp(nowMs);
        spatId.setIntersectionId(spat.getIntersectionId());
        spatId.setRoadRegulatorId(spat.getRegion());

        if (spat.getIntersectionId() != null) {
            event.setIntersectionID(spat.getIntersectionId());
        }
        if (spat.getRegion() != null) {
            event.setRoadRegulatorID(spat.getRegion());
        } else {
            event.setRoadRegulatorID(-1);
        }

        Set<RegulatorIntersectionId> mapIdSet = new HashSet<>();
        mapIdSet.add(mapId);
        event.setMapRegulatorIntersectionIds(mapIdSet);

        Set<RegulatorIntersectionId> spatIdSet = new HashSet<>();
        spatIdSet.add(spatId);
        event.setSpatRegulatorIntersectionIds(spatIdSet);

        if (!event.getSpatRegulatorIntersectionIds().equals(event.getMapRegulatorIntersectionIds())) {
            String gateKey = rsuKey + "|" + spatId.getIntersectionId() + "|" + spatId.getRoadRegulatorId();
            if (referenceAlignmentGate.shouldEmit(gateKey, mapIdSet, spatIdSet, nowMs)) {
                events.add(new KeyValue<>(rsuKey, event));
            }
        }

        return events;
    }

    public Topology buildTopology() {

        // Populate concurrent permissive allowed from intersection-level config
        ConfigMap<ConnectedLanesPairList> concurrentPermissiveConfigMap = parameters.getConcurrentPermissiveListMap();
        final Set<ConnectedLanesPair> allowConcurrentPermissiveSet = new HashSet<>();
        for (ConnectedLanesPairList list : concurrentPermissiveConfigMap.values()) {
            allowConcurrentPermissiveSet.addAll(list);
        }

        referenceAlignmentGate = new AlignmentEmitGate(parameters.getAlignmentSampleIntervalMs());
        signalGroupAlignmentGate = new AlignmentEmitGate(parameters.getAlignmentSampleIntervalMs());
        assessmentCacheManager.clear();

        StreamsBuilder builder = new StreamsBuilder();

        // SPaT Input Stream
        KStream<RsuIntersectionKey, ProcessedSpat> processedSpatStream = builder.stream(
                parameters.getSpatInputTopicName(),
                Consumed.with(
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSpat())
        );

        // Map table keyed by RsuIntersectionKey
        KTable<RsuIntersectionKey, ProcessedMap<LineString>> mapKTable = builder.table(parameters.getMapInputTopicName(),
                Materialized.with(
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedMapGeoJson()));

        // RSU-keyed MAP table used only for reference-alignment miss path (SPaT key has no matching MAP)
        KTable<String, ProcessedMap<LineString>> mapKTableRsuKey =
            mapKTable.toStream().selectKey((key, value) -> key.getRsuId()).toTable(
                Materialized.with(Serdes.String(), us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedMapGeoJson())
        );

        // Primary left join: one MAP lookup per SPaT for matching intersection keys
        KStream<RsuIntersectionKey, SpatMap> spatJoinedMap = processedSpatStream
                .leftJoin(mapKTable, (spat, map) -> new SpatMap(spat, map),
                        Joined.<RsuIntersectionKey, ProcessedSpat, ProcessedMap<LineString>>as("spat-maps-joined")
                                .withKeySerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey())
                                .withValueSerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSpat())
                                .withOtherValueSerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedMapGeoJson()));

        KStream<RsuIntersectionKey, SpatMap> spatWithMap = spatJoinedMap
                .filter((key, value) -> value != null && value.getMap() != null && value.getSpat() != null);
        KStream<RsuIntersectionKey, SpatMap> spatWithoutMap = spatJoinedMap
                .filter((key, value) -> value != null && value.getSpat() != null && value.getMap() == null);

        // Intersection Reference Alignment: matched keys use the primary join; misses use RSU-only lookup
        KStream<String, IntersectionReferenceAlignmentEvent> referenceAlignmentMatched =
                spatWithMap
                        .selectKey((key, value) -> key.getRsuId())
                        .flatMap((key, value) -> evaluateIntersectionReferenceAlignment(key, value));

        KStream<String, IntersectionReferenceAlignmentEvent> referenceAlignmentMiss =
                spatWithoutMap
                        .selectKey((key, value) -> key.getRsuId())
                        .leftJoin(mapKTableRsuKey,
                                (spatMap, map) -> new SpatMap(spatMap.getSpat(), map),
                                Joined.<String, SpatMap, ProcessedMap<LineString>>as("spat-maps-joined-rsu-miss")
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(JsonSerdes.SpatMap())
                                        .withOtherValueSerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedMapGeoJson()))
                        .flatMap((key, value) -> evaluateIntersectionReferenceAlignment(key, value));

        KStream<String, IntersectionReferenceAlignmentEvent> intersectionReferenceAlignmentEventStream =
                referenceAlignmentMatched.merge(referenceAlignmentMiss);

        if (parameters.isAggregateIntersectionReferenceAlignmentEvents()) {
            // Aggregate Intersection Reference Alignment
            KStream<String, IntersectionReferenceAlignmentEventAggregation> aggEventStream =
                intersectionReferenceAlignmentAggregationAlgorithm.buildTopology(
                        builder, intersectionReferenceAlignmentEventStream);

            // Notifications, aggregated
            buildIntersectionReferenceAlignmentNotificationAggregationTopology(aggEventStream);
        } else {
            // Don't aggregate Intersection Reference Alignment
            intersectionReferenceAlignmentEventStream.to(
                    parameters.getIntersectionReferenceAlignmentEventTopicName(),
                    Produced.with(Serdes.String(),
                            JsonSerdes.IntersectionReferenceAlignmentEvent()));

            // Notifications, non aggregated
            buildIntersectionReferenceAlignmentNotificationTopology(intersectionReferenceAlignmentEventStream);
        }

        // Signal Group Alignment Event Check (MAP-derived signal groups cached by revision)
        KStream<RsuIntersectionKey, SignalGroupAlignmentEvent> signalGroupAlignmentEventStream = spatWithMap.flatMap(
                (key, value) -> {
                    ArrayList<KeyValue<RsuIntersectionKey, SignalGroupAlignmentEvent>> events = new ArrayList<>();
                    ProcessedSpat spat = value.getSpat();
                    MapDerivedAssessmentCache cache = assessmentCacheManager.getOrBuild(
                            key.toString(), value.getMap(), allowConcurrentPermissiveSet);

                    Set<Integer> spatSignalGroups = new HashSet<>();
                    if (spat.getStates() != null) {
                        for (ProcessedMovementState state : spat.getStates()) {
                            spatSignalGroups.add(state.getSignalGroup());
                        }
                    }
                    Set<Integer> mapSignalGroups = cache.getMapSignalGroups();

                    if (!mapSignalGroups.equals(spatSignalGroups)) {
                        long nowMs = SpatTimestampExtractor.getSpatTimestamp(spat);
                        if (!signalGroupAlignmentGate.shouldEmit(
                                key.toString(), mapSignalGroups, spatSignalGroups, nowMs)) {
                            return events;
                        }

                        SignalGroupAlignmentEvent event = new SignalGroupAlignmentEvent();
                        event.setSource(key.getRsuId());
                        event.setTimestamp(nowMs);

                        if (spat.getIntersectionId() != null) {
                            event.setIntersectionID(spat.getIntersectionId());
                        }
                        if (spat.getRegion() != null) {
                            event.setRoadRegulatorID(spat.getRegion());
                        } else {
                            event.setRoadRegulatorID(-1);
                        }
                        event.setMapSignalGroupIds(new HashSet<>(mapSignalGroups));
                        event.setSpatSignalGroupIds(spatSignalGroups);
                        events.add(new KeyValue<>(key, event));
                    }

                    return events;
                });

        if (parameters.isAggregateSignalGroupAlignmentEvents()) {
            // Aggregate Signal Group Alignment events
            KStream<RsuIntersectionKey, SignalGroupAlignmentEventAggregation> aggEventStream =
                signalGroupAlignmentAggregationAlgorithm.buildTopology(builder, signalGroupAlignmentEventStream);

            // Notifications, aggregated
            buildSignalGroupAlignmentNotificationAggregationTopology(aggEventStream);
        } else {
            // Don't aggregate Signal Group Alignment events
            signalGroupAlignmentEventStream.to(
                    parameters.getSignalGroupAlignmentEventTopicName(),
                    Produced.with(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                            JsonSerdes.SignalGroupAlignmentEvent(),
                            new IntersectionIdPartitioner<>()));

            // Notifications, non aggregated
            buildSignalGroupAlignmentNotificationTopology(signalGroupAlignmentEventStream);
        }


        // Signal State Conflict Event Check — evaluate SPaT states against precomputed MAP conflict pairs
        KStream<RsuIntersectionKey, SignalStateConflictEvent> signalStateConflictEventStream = spatWithMap.flatMap(
                (key, value) -> {

                    ArrayList<KeyValue<RsuIntersectionKey, SignalStateConflictEvent>> events = new ArrayList<>();

                    ProcessedMap<LineString> map = value.getMap();
                    ProcessedSpat spat = value.getSpat();

                    MapDerivedAssessmentCache cache = assessmentCacheManager.getOrBuild(
                            key.toString(), map, allowConcurrentPermissiveSet);

                    Set<Integer> revocableLaneIds = cache.getRevocableLaneIds();
                    Set<Integer> enabledLanes = spat.getEnabledLanes() != null
                            ? new HashSet<>(spat.getEnabledLanes())
                            : Set.of();

                    for (ConflictingConnectionPair pair : cache.getConflictPairs()) {
                        boolean firstDisabled = isRevocableDisabled(
                                pair.getFirstIngressLaneId(), pair.getFirstEgressLaneId(),
                                revocableLaneIds, enabledLanes);
                        boolean secondDisabled = isRevocableDisabled(
                                pair.getSecondIngressLaneId(), pair.getSecondEgressLaneId(),
                                revocableLaneIds, enabledLanes);
                        if (firstDisabled || secondDisabled) {
                            log.debug("For key: {}, conflicting pair involves revocable disabled lanes. Skipping.", key);
                            continue;
                        }

                        ProcessedMovementPhaseState firstState = getSpatEventStateBySignalGroup(spat,
                                pair.getFirstSignalGroup());
                        ProcessedMovementPhaseState secondState = getSpatEventStateBySignalGroup(spat,
                                pair.getSecondSignalGroup());

                        if (firstState == null || secondState == null) {
                            continue;
                        }

                        if (doStatesConflict(firstState, secondState)) {
                            SignalStateConflictEvent event = new SignalStateConflictEvent();
                            event.setTimestamp(SpatTimestampExtractor.getSpatTimestamp(spat));
                            event.setRoadRegulatorID(cache.getRoadRegulatorId());
                            event.setIntersectionID(cache.getIntersectionId());
                            event.setFirstConflictingSignalGroup(pair.getFirstSignalGroup());
                            event.setSecondConflictingSignalGroup(pair.getSecondSignalGroup());
                            event.setFirstConflictingSignalState(firstState);
                            event.setSecondConflictingSignalState(secondState);
                            event.setSource(key.toString());

                            if (firstState.equals(ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED)
                                    || firstState.equals(ProcessedMovementPhaseState.PROTECTED_CLEARANCE)) {
                                event.setConflictType(secondState);
                            } else {
                                event.setConflictType(firstState);
                            }

                            events.add(new KeyValue<>(key, event));
                        }
                    }

                    return events;
                });

        // Revocable Enabled Lane Alignment Algorithm uses the joined Spat/Map stream (map present)
        revocableEnabledLaneAlignmentAlgorithm.buildTopology(builder, spatWithMap);

        if (parameters.isAggregateSignalStateConflictEvents()) {
            // Aggregate Signal State Conflict events
            // New key includes all fields to aggregate on
            var signalStateConflictAggKeyStream
                    = signalStateConflictEventStream.selectKey((key, value) -> {
                     var aggKey = new SignalStateConflictAggregationKey();
                     aggKey.setRsuId(key.getRsuId());
                     aggKey.setIntersectionId(key.getIntersectionId());
                     aggKey.setRegion(key.getRegion());
                     aggKey.setEventStateA(value.getFirstConflictingSignalState());
                     aggKey.setEventStateB(value.getSecondConflictingSignalState());
                     aggKey.setConflictingSignalGroupA(value.getFirstConflictingSignalGroup());
                     aggKey.setConflictingSignalGroupB(value.getSecondConflictingSignalGroup());
                     return aggKey;
            })
            // Use same partitioner so that repartition on new key will
            // not actually change the partitions of any items
            .repartition(
                    Repartitioned.with(JsonSerdes.SignalStateConflictAggregationKey(),
                            JsonSerdes.SignalStateConflictEvent())
                            .withStreamPartitioner(new IntersectionIdPartitioner<>()));

            KStream<SignalStateConflictAggregationKey, SignalStateConflictEventAggregation> aggEventStream =
                signalStateConflictAggregationAlgorithm.buildTopology(builder, signalStateConflictAggKeyStream);

            // Notifications, aggregated
            buildSignalStateConflictNotificationAggregationTopology(aggEventStream);
        } else {
            // Don't aggregate Signal State Conflict events
            signalStateConflictEventStream.to(
                    parameters.getSignalStateConflictEventTopicName(),
                    Produced.with(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                            JsonSerdes.SignalStateConflictEvent(),
                            new IntersectionIdPartitioner<>()));

            // Notifications, not aggregated
            buildSignalStateConflictNotificationTopology(signalStateConflictEventStream);
        }

        if(parameters.isDebug()){
            signalStateConflictEventStream.print(Printed.toSysOut());
        }

        return builder.build(streamsProperties);

    }

    private void buildIntersectionReferenceAlignmentNotificationTopology(
            KStream<String, IntersectionReferenceAlignmentEvent> eventStream) {

        KStream<String, IntersectionReferenceAlignmentNotification> notificationEventStream = eventStream
                .flatMap(
                        (key, value) -> {
                            List<KeyValue<String, IntersectionReferenceAlignmentNotification>> result = new ArrayList<KeyValue<String, IntersectionReferenceAlignmentNotification>>();

                            var notification = new IntersectionReferenceAlignmentNotification();
                            notification.setEvent(value);
                            notification.setNotificationText(
                                    "Intersection Reference Alignment Notification, generated because corresponding intersection reference alignment event was generated.");
                            notification.setNotificationHeading("Intersection Reference Alignment");
                            result.add(new KeyValue<>(key, notification));
                            return result;
                        });

        KTable<String, IntersectionReferenceAlignmentNotification> intersectionNotificationTable = notificationEventStream
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.IntersectionReferenceAlignmentNotification()))
                .reduce(
                        (oldValue, newValue) -> {
                            return newValue;
                        },
                        Materialized
                                .<String, IntersectionReferenceAlignmentNotification, KeyValueStore<Bytes, byte[]>>as(
                                        "IntersectionReferenceAlignmentNotification")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.IntersectionReferenceAlignmentNotification()));

        intersectionNotificationTable.toStream().to(
                parameters.getIntersectionReferenceAlignmentNotificationTopicName(),
                Produced.with(Serdes.String(),
                        JsonSerdes.IntersectionReferenceAlignmentNotification()));
    }

    private void buildIntersectionReferenceAlignmentNotificationAggregationTopology(
            KStream<String, IntersectionReferenceAlignmentEventAggregation> aggEventStream) {

        aggEventStream
                .mapValues(aggEvent -> {
                    var aggNotification = new IntersectionReferenceAlignmentNotificationAggregation();
                    aggNotification.setEventAggregation(aggEvent);
                    aggNotification.setNotificationText(
                            "Intersection Reference Alignment Notification, generated because one or more" +
                                    " corresponding intersection reference alignment events were generated.");
                    aggNotification.setNotificationHeading("Intersection Reference Alignment");
                    return aggNotification;
                })
                .toTable(
                        Materialized.<
                                String,
                                IntersectionReferenceAlignmentNotificationAggregation,
                                KeyValueStore<Bytes, byte[]>>as(
                                    "IntersectionReferenceAlignmentNotificationAggregation")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.IntersectionReferenceAlignmentNotificationAggregation())
                )
                .toStream()
                .to(parameters.getIntersectionReferenceAlignmentNotificationAggTopicName(),
                        Produced.with(
                                Serdes.String(),
                                JsonSerdes.IntersectionReferenceAlignmentNotificationAggregation()
                        ));

    }

    private void buildSignalGroupAlignmentNotificationTopology(
            KStream<RsuIntersectionKey, SignalGroupAlignmentEvent> eventStream) {

        KStream<RsuIntersectionKey, SignalGroupAlignmentNotification> signalGroupNotificationEventStream = eventStream
                .flatMap(
                        (key, value) -> {
                            List<KeyValue<RsuIntersectionKey, SignalGroupAlignmentNotification>> result = new ArrayList<>();

                            SignalGroupAlignmentNotification notification = new SignalGroupAlignmentNotification();
                            notification.setEvent(value);
                            notification.setNotificationText(
                                    "Signal Group Alignment Notification, generated because corresponding signal group alignment event was generated.");
                            notification.setNotificationHeading("Signal Group Alignment");
                            result.add(new KeyValue<>(key, notification));
                            return result;
                        });

        KTable<RsuIntersectionKey, SignalGroupAlignmentNotification> signalGroupNotificationTable = signalGroupNotificationEventStream
                .groupByKey(Grouped.with(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(), JsonSerdes.SignalGroupAlignmentNotification()))
                .reduce(
                        (oldValue, newValue) -> {
                            return newValue;
                        },
                        Materialized
                                .<RsuIntersectionKey, SignalGroupAlignmentNotification, KeyValueStore<Bytes, byte[]>>as(
                                        "SignalGroupAlignmentNotification")
                                .withKeySerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey())
                                .withValueSerde(JsonSerdes.SignalGroupAlignmentNotification()));

        signalGroupNotificationTable.toStream().to(
                parameters.getSignalGroupAlignmentNotificationTopicName(),
                Produced.with(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                        JsonSerdes.SignalGroupAlignmentNotification(),
                        new IntersectionIdPartitioner<>()));
    }

    private void buildSignalGroupAlignmentNotificationAggregationTopology(
            KStream<RsuIntersectionKey, SignalGroupAlignmentEventAggregation> aggEventStream) {

        aggEventStream
                .mapValues(aggEvent -> {
                    var aggNotification = new SignalGroupAlignmentNotificationAggregation();
                    aggNotification.setEventAggregation(aggEvent);
                    aggNotification.setNotificationText(
                            "Signal Group Alignment Notification, generated because one or more corresponding signal" +
                                    " group alignment events were generated.");
                    aggNotification.setNotificationHeading("Signal Group Alignment");
                    return aggNotification;
                })
                .toTable(
                        Materialized.<RsuIntersectionKey,
                                SignalGroupAlignmentNotificationAggregation,
                                KeyValueStore<Bytes, byte[]>>as(
                                        "SignalGroupAlignmentNotificationAggregation")
                                .withKeySerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey())
                                .withValueSerde(JsonSerdes.SignalGroupAlignmentNotificationAggregation()))
                .toStream()
                .to(parameters.getSignalGroupAlignmentNotificationAggTopicName(),
                        Produced.with(
                                us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                                JsonSerdes.SignalGroupAlignmentNotificationAggregation(),
                                new IntersectionIdPartitioner<>()));
    }

    private void buildSignalStateConflictNotificationTopology(
            KStream<RsuIntersectionKey, SignalStateConflictEvent>  eventStream) {

        KStream<RsuIntersectionKey, SignalStateConflictNotification> signalStateConflictNotificationStream
                = eventStream
                .flatMap(
                        (key, value) -> {
                            List<KeyValue<RsuIntersectionKey, SignalStateConflictNotification>> result = new ArrayList<>();

                            SignalStateConflictNotification notification = new SignalStateConflictNotification();
                            notification.setEvent(value);
                            notification.setNotificationText(
                                    "Signal State Conflict Notification, generated because corresponding signal state conflict event was generated.");
                            notification.setNotificationHeading("Signal State Conflict");
                            result.add(new KeyValue<>(key, notification));
                            return result;
                        });

        if(parameters.isDebug()){
            signalStateConflictNotificationStream.print(Printed.toSysOut());
        }

        KTable<RsuIntersectionKey, SignalStateConflictNotification> signalStateConflictNotificationTable = signalStateConflictNotificationStream
                .groupByKey(Grouped.with(
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                        JsonSerdes.SignalStateConflictNotification()))
                .reduce(
                        (oldValue, newValue) -> {
                            return newValue;
                        },
                        Materialized
                                .<RsuIntersectionKey, SignalStateConflictNotification, KeyValueStore<Bytes, byte[]>>as(
                                        "SignalStateConflictNotification")
                                .withKeySerde(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey())
                                .withValueSerde(JsonSerdes.SignalStateConflictNotification()));


        signalStateConflictNotificationTable.toStream().to(
                parameters.getSignalStateConflictNotificationTopicName(),
                Produced.with(us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                        JsonSerdes.SignalStateConflictNotification(),
                        new IntersectionIdPartitioner<>()));

    }

    private void buildSignalStateConflictNotificationAggregationTopology(
            KStream<SignalStateConflictAggregationKey, SignalStateConflictEventAggregation> aggEventStream) {

        aggEventStream
                .mapValues(aggEvent -> {
                    var aggNotification = new SignalStateConflictNotificationAggregation();
                    aggNotification.setEventAggregation(aggEvent);
                    aggNotification.setNotificationText(
                            "Signal State Conflict Notification, generated because corresponding signal state conflict" +
                                    " event was generated.");
                    aggNotification.setNotificationHeading("Signal State Conflict");
                    return aggNotification;
                })
                .toTable(
                        Materialized.<SignalStateConflictAggregationKey,
                                SignalStateConflictNotificationAggregation,
                                KeyValueStore<Bytes, byte[]>>as(
                                        "SignalStateConflictNotificationAggregation")
                                .withKeySerde(JsonSerdes.SignalStateConflictAggregationKey())
                                .withValueSerde(JsonSerdes.SignalStateConflictNotificationAggregation()))
                .toStream()
                .to(parameters.getSignalStateConflictNotificationAggTopicName(),
                        Produced.with(
                             JsonSerdes.SignalStateConflictAggregationKey(),
                             JsonSerdes.SignalStateConflictNotificationAggregation(),
                             new IntersectionIdPartitioner<>()));

    }

    @Override
    public void setIntersectionReferenceAlignmentAggregationAlgorithm(
            IntersectionReferenceAlignmentAggregationAlgorithm intersectionReferenceAlignmentAggregationAlgorithm) {
        // Enforce the algorithm being a Streams algorithm
        if (intersectionReferenceAlignmentAggregationAlgorithm instanceof IntersectionReferenceAlignmentAggregationStreamsAlgorithm streamsAlgorithm) {
            this.intersectionReferenceAlignmentAggregationAlgorithm = streamsAlgorithm;
        } else {
            throw new IllegalArgumentException("Intersection Reference Alignment Aggregation algorithm must be a Streams algorithm");
        }
    }

    @Override
    public void setSignalGroupAlignmentAggregationAlgorithm(
            SignalGroupAlignmentAggregationAlgorithm signalGroupAlignmentAggregationAlgorithm) {
        // Enforce the algorithm being a Streams algorithm
        if (signalGroupAlignmentAggregationAlgorithm instanceof SignalGroupAlignmentAggregationStreamsAlgorithm streamsAlgorithm) {
            this.signalGroupAlignmentAggregationAlgorithm = streamsAlgorithm;
        } else {
            throw new IllegalArgumentException("Signal Group Alignment Aggregation algorithm must be a Streams algorithm");
        }
    }

    @Override
    public void setSignalStateConflictAggregationAlgorithm(
            SignalStateConflictAggregationAlgorithm signalStateConflictAggregationAlgorithm) {
        // Enforce the algorithm being a Streams algorithm
        if (signalStateConflictAggregationAlgorithm instanceof SignalStateConflictAggregationStreamsAlgorithm streamsAlgorithm) {
            this.signalStateConflictAggregationAlgorithm = streamsAlgorithm;
        } else {
            throw new IllegalArgumentException("Signal State Conflict Aggregation algorithm must be a Streams algorithm");
        }
    }

    @Override
    public void setRevocableEnabledLaneAlignmentAlgorithm(RevocableEnabledLaneAlignmentAlgorithm revocableEnabledLaneAlignmentAlgorithm) {
        // Enforce the algorithm being a Streams algorithm
        if (revocableEnabledLaneAlignmentAlgorithm instanceof RevocableEnabledLaneAlignmentStreamsAlgorithm streamsAlgorithm) {
            this.revocableEnabledLaneAlignmentAlgorithm = streamsAlgorithm;
        } else {
            throw new IllegalArgumentException("Revocable Enabled Lane Alignment Algorithm must be a Streams algorithm");
        }
    }

}
