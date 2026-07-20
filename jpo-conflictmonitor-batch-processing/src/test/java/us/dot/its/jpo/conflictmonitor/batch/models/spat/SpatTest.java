package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import org.junit.jupiter.api.Test;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementEvent;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Covers Spat#fromProcessedSpat: mapping a raw ProcessedSpat into this project's
 * simplified per-signal-group model.
 */
class SpatTest {

    private ProcessedMovementState state(int signalGroup, ProcessedMovementPhaseState eventState) {
        var state = new ProcessedMovementState();
        state.setSignalGroup(signalGroup);
        var event = new ProcessedMovementEvent();
        event.setEventState(eventState);
        state.setStateTimeSpeed(List.of(event));
        return state;
    }

    @Test
    void fromProcessedSpatUsesUtcTimeStampTSAsTheTimestamp() {
        Instant timestamp = Instant.parse("2026-05-03T10:00:00Z");
        var spat = new ProcessedSpat();
        spat.setUtcTimeStampTS(timestamp);
        spat.setStates(List.of());

        Spat result = Spat.fromProcessedSpat(spat);

        assertThat(result.getTimestamp(), is(timestamp));
    }

    @Test
    void fromProcessedSpatMapsAndSortsStatesBySignalGroup() {
        Instant timestamp = Instant.parse("2026-05-03T10:00:00Z");
        var spat = new ProcessedSpat();
        spat.setUtcTimeStampTS(timestamp);
        spat.setStates(List.of(
                state(3, ProcessedMovementPhaseState.STOP_AND_REMAIN),
                state(1, ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED)));

        Spat result = Spat.fromProcessedSpat(spat);

        assertThat(result.getStates(), hasSize(2));
        assertThat(result.getStates().get(0).getSignalGroup(), is(1));
        assertThat(result.getStates().get(1).getSignalGroup(), is(3));
        assertThat(result.getStates().get(0).getTimestamp(), is(timestamp));
    }

    @Test
    void fromProcessedSpatLeavesTimestampNullWhenUtcTimeStampTSIsNull() {
        var spat = new ProcessedSpat();
        spat.setUtcTimeStampTS(null);
        spat.setStates(List.of());

        Spat result = Spat.fromProcessedSpat(spat);

        assertThat(result.getTimestamp(), is(nullValue()));
    }
}
