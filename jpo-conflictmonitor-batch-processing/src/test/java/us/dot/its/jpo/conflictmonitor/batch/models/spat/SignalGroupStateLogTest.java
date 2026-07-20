package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import org.junit.jupiter.api.Test;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Covers SignalGroupStateLog#fromSpatLog: reducing a raw SPaT time series per signal
 * group down to state-change ("delta") events.
 */
class SignalGroupStateLogTest {

    private static final Instant T0 = Instant.parse("2026-05-03T10:00:00Z");

    private SignalGroupState state(int signalGroup, Instant timestamp, ProcessedMovementPhaseState eventState) {
        var s = new SignalGroupState();
        s.setSignalGroup(signalGroup);
        s.setTimestamp(timestamp);
        s.setEventState(eventState);
        return s;
    }

    private Spat spat(Instant timestamp, SignalGroupState... states) {
        var spat = new Spat();
        spat.setTimestamp(timestamp);
        spat.setStates(List.of(states));
        return spat;
    }

    private SpatLog spatLog(List<Spat> spats) {
        var log = new SpatLog();
        log.setIntersectionId(100);
        log.setStartTime(T0);
        log.setEndTime(T0.plusSeconds(60));
        log.setSpats(spats);
        return log;
    }

    @Test
    void keepsOnlyTheFirstOccurrenceOfEachConsecutiveStateChange() {
        SpatLog spatLog = spatLog(List.of(
                spat(T0, state(1, T0, ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED)),
                spat(T0.plusSeconds(10), state(1, T0.plusSeconds(10), ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED)),
                spat(T0.plusSeconds(20), state(1, T0.plusSeconds(20), ProcessedMovementPhaseState.PROTECTED_CLEARANCE))));

        SignalGroupStateLog result = SignalGroupStateLog.fromSpatLog(spatLog);
        List<TimestampedState> deltas = result.getSignalGroupStates().get(1);

        assertThat(deltas, hasSize(2));
        assertThat(deltas.getFirst().getTimestamp(), is(T0));
        assertThat(deltas.getFirst().getEventState(), is(ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED));
        assertThat(deltas.get(1).getTimestamp(), is(T0.plusSeconds(20)));
        assertThat(deltas.get(1).getEventState(), is(ProcessedMovementPhaseState.PROTECTED_CLEARANCE));
    }

    @Test
    void keepsEveryStateWhenEveryStateDiffersFromThePrevious() {
        SpatLog spatLog = spatLog(List.of(
                spat(T0, state(1, T0, ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED)),
                spat(T0.plusSeconds(10), state(1, T0.plusSeconds(10), ProcessedMovementPhaseState.PROTECTED_CLEARANCE)),
                spat(T0.plusSeconds(20), state(1, T0.plusSeconds(20), ProcessedMovementPhaseState.STOP_AND_REMAIN))));

        SignalGroupStateLog result = SignalGroupStateLog.fromSpatLog(spatLog);

        assertThat(result.getSignalGroupStates().get(1), hasSize(3));
    }

    @Test
    void tracksEachSignalGroupIndependently() {
        SpatLog spatLog = spatLog(List.of(
                spat(T0,
                        state(1, T0, ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED),
                        state(2, T0, ProcessedMovementPhaseState.STOP_AND_REMAIN)),
                spat(T0.plusSeconds(10),
                        state(1, T0.plusSeconds(10), ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED), // unchanged
                        state(2, T0.plusSeconds(10), ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED)))); // changed

        SignalGroupStateLog result = SignalGroupStateLog.fromSpatLog(spatLog);

        assertThat(result.getSignalGroupStates().get(1), hasSize(1));
        assertThat(result.getSignalGroupStates().get(2), hasSize(2));
    }

    @Test
    void handlesEmptySpatList() {
        SpatLog spatLog = spatLog(new ArrayList<>());

        SignalGroupStateLog result = SignalGroupStateLog.fromSpatLog(spatLog);

        assertThat(result.getSignalGroupStates().keySet(), is(empty()));
    }
}
