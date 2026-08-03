package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import org.junit.jupiter.api.Test;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementEvent;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementState;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Covers SignalGroupState#fromMovementState.
 */
class SignalGroupStateTest {

    private static final Instant TIMESTAMP = Instant.parse("2026-05-03T10:00:00Z");

    private ProcessedMovementEvent event(ProcessedMovementPhaseState eventState) {
        var event = new ProcessedMovementEvent();
        event.setEventState(eventState);
        return event;
    }

    @Test
    void fromMovementStateMapsSignalGroupAndEventState() {
        var state = new ProcessedMovementState();
        state.setSignalGroup(2);
        state.setStateTimeSpeed(List.of(event(ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED)));

        SignalGroupState result = SignalGroupState.fromMovementState(state, TIMESTAMP);

        assertThat(result.getSignalGroup(), is(2));
        assertThat(result.getTimestamp(), is(TIMESTAMP));
        assertThat(result.getEventState(), is(ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED));
    }

    @Test
    void fromMovementStateUsesTheFirstStateTimeSpeedEntryWhenMultipleArePresent() {
        // Documents current behavior - only stateTimeSpeed.getFirst() is used, any other
        // concurrent movement events for the signal group are ignored.
        var state = new ProcessedMovementState();
        state.setSignalGroup(2);
        state.setStateTimeSpeed(List.of(
                event(ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED),
                event(ProcessedMovementPhaseState.STOP_AND_REMAIN)));

        SignalGroupState result = SignalGroupState.fromMovementState(state, TIMESTAMP);

        assertThat(result.getEventState(), is(ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED));
    }
}
