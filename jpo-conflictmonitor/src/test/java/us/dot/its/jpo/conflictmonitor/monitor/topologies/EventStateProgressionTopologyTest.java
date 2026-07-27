package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.junit.Test;
import org.testng.collections.Lists;
import us.dot.its.jpo.conflictmonitor.monitor.algorithms.event_state_progression.EventStateProgressionParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.event_state_progression.PhaseStateTransition;
import us.dot.its.jpo.conflictmonitor.monitor.models.event_state_progression.PhaseStateTransitionList;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.EventStateProgressionEvent;
import us.dot.its.jpo.conflictmonitor.monitor.serialization.JsonSerdes;
import us.dot.its.jpo.geojsonconverter.partitioner.RsuIntersectionKey;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementEvent;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EventStateProgressionTopologyTest {

    final String inputTopic = "topic.ProcessedSpat";
    final String outputTopic = "topic.CmEventStateProgressionEvent";
    final String notificationTopic = "topic.CmEventStateProgressionNotification";

    @Test
    public void unchangedPhaseProducesNoEvents() {
        EventStateProgressionTopology topology = newTopology();
        Topology built = build(topology);

        try (TopologyTestDriver driver = new TopologyTestDriver(built)) {
            var input = driver.createInputTopic(
                    inputTopic,
                    us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey().serializer(),
                    us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSpat().serializer());
            TestOutputTopic<?, EventStateProgressionEvent> output = driver.createOutputTopic(
                    outputTopic,
                    JsonSerdes.RsuIntersectionSignalGroupKey().deserializer(),
                    JsonSerdes.EventStateProgressionEvent().deserializer());

            RsuIntersectionKey key = new RsuIntersectionKey("127.0.0.1", 12109, 0);
            long t0 = 1_700_000_000_000L;
            for (int i = 0; i < 20; i++) {
                input.pipeInput(key, spat(t0 + i * 100L, ProcessedMovementPhaseState.STOP_AND_REMAIN), t0 + i * 100L);
            }

            assertTrue(output.isEmpty(), "stable phase must not emit progression events");
        }
    }

    @Test
    public void illegalPhaseChangeProducesEvent() {
        EventStateProgressionTopology topology = newTopology();
        Topology built = build(topology);

        try (TopologyTestDriver driver = new TopologyTestDriver(built)) {
            var input = driver.createInputTopic(
                    inputTopic,
                    us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey().serializer(),
                    us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSpat().serializer());
            TestOutputTopic<?, EventStateProgressionEvent> output = driver.createOutputTopic(
                    outputTopic,
                    JsonSerdes.RsuIntersectionSignalGroupKey().deserializer(),
                    JsonSerdes.EventStateProgressionEvent().deserializer());

            RsuIntersectionKey key = new RsuIntersectionKey("127.0.0.1", 12109, 0);
            long t0 = 1_700_000_000_000L;

            // Protected green, then red (illegal). Advance to put+grace so both are queryable.
            input.pipeInput(key, spat(t0, ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED), t0);
            input.pipeInput(key, spat(t0 + 200L, ProcessedMovementPhaseState.STOP_AND_REMAIN), t0 + 200L);
            // Same phase at put+grace — query completes transition detection without another put
            input.pipeInput(key, spat(t0 + 300L, ProcessedMovementPhaseState.STOP_AND_REMAIN), t0 + 300L);

            var results = output.readKeyValuesToList();
            assertEquals(1, results.size());
            EventStateProgressionEvent event = results.getFirst().value;
            assertEquals(ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED, event.getEventStateA());
            assertEquals(ProcessedMovementPhaseState.STOP_AND_REMAIN, event.getEventStateB());
            assertEquals(5, event.getSignalGroupID());
        }
    }

    private EventStateProgressionTopology newTopology() {
        EventStateProgressionParameters parameters = new EventStateProgressionParameters();
        parameters.setDebug(false);
        parameters.setOutputTopicName(outputTopic);
        parameters.setNotificationTopicName(notificationTopic);
        parameters.setAggNotificationTopicName("topic.CmEventStateProgressionNotificationAggregation");
        parameters.setMovementStateStoreName("spatTransitionStateStore");
        parameters.setLatestTransitionStoreName("latestTransitionStateStore");
        parameters.setLatestPhaseStoreName("latestPhaseStateStore");
        parameters.setBufferTimeMs(1000);
        parameters.setBufferGracePeriodMs(100);
        parameters.setAggregateEvents(false);

        PhaseStateTransitionList illegal = new PhaseStateTransitionList();
        PhaseStateTransition t = new PhaseStateTransition();
        t.setStateA(ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED);
        t.setStateB(ProcessedMovementPhaseState.STOP_AND_REMAIN);
        illegal.add(t);
        parameters.setIllegalSpatTransitionList(illegal);

        EventStateProgressionTopology topology = new EventStateProgressionTopology();
        topology.setParameters(parameters);
        return topology;
    }

    private Topology build(EventStateProgressionTopology topology) {
        StreamsBuilder builder = new StreamsBuilder();
        var stream = builder.stream(
                inputTopic,
                Consumed.with(
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.RsuIntersectionKey(),
                        us.dot.its.jpo.geojsonconverter.serialization.JsonSerdes.ProcessedSpat()));
        topology.buildTopology(builder, stream);
        return builder.build();
    }

    private ProcessedSpat spat(long utcMs, ProcessedMovementPhaseState phase) {
        ProcessedSpat spat = new ProcessedSpat();
        spat.setUtcTimeStamp(ZonedDateTime.ofInstant(Instant.ofEpochMilli(utcMs), ZoneOffset.UTC));
        spat.setIntersectionId(12109);
        spat.setRegion(0);

        ProcessedMovementState state = new ProcessedMovementState();
        state.setSignalGroup(5);
        ProcessedMovementEvent event = new ProcessedMovementEvent();
        event.setEventState(phase);
        state.setStateTimeSpeed(Lists.newArrayList(event));
        spat.setStates(Lists.newArrayList(state));
        return spat;
    }
}
