package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import org.junit.jupiter.api.Test;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Covers SignalGroupIndicationLog#fromSignalGroupStateLog: converting SPaT state deltas
 * into RED/YELLOW/GREEN indications for pairing against ATSPM events.
 */
class SignalGroupIndicationLogTest {

    private static final Instant T0 = Instant.parse("2026-05-03T10:00:00Z");

    private TimestampedState ts(Instant timestamp, ProcessedMovementPhaseState state) {
        return new TimestampedState(timestamp, state);
    }

    private SignalGroupStateLog stateLog(Map<Integer, List<TimestampedState>> states) {
        var log = new SignalGroupStateLog();
        log.setIntersectionId(100);
        log.setStartTime(T0);
        log.setEndTime(T0.plusSeconds(60));
        log.setSignalGroupStates(states);
        return log;
    }

    @Test
    void convertsMappedStatesToIndicationsPerSignalGroup() {
        Map<Integer, List<TimestampedState>> states = new HashMap<>();
        states.put(1, List.of(
                ts(T0, ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED),
                ts(T0.plusSeconds(30), ProcessedMovementPhaseState.PROTECTED_CLEARANCE)));

        SignalGroupIndicationLog result = SignalGroupIndicationLog.fromSignalGroupStateLog(stateLog(states));
        List<TimestampedIndication> indications = result.getIndicationsMap().getIndications(1);

        assertThat(indications, hasSize(2));
        assertThat(indications.get(0).getIndication(), is(SpatSignalIndication.GREEN));
        assertThat(indications.get(1).getIndication(), is(SpatSignalIndication.YELLOW));
    }

    @Test
    void dropsStateChangesThatDoNotMapToAnIndication() {
        Map<Integer, List<TimestampedState>> states = new HashMap<>();
        states.put(2, List.of(
                ts(T0, ProcessedMovementPhaseState.DARK),
                ts(T0.plusSeconds(10), ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED)));

        SignalGroupIndicationLog result = SignalGroupIndicationLog.fromSignalGroupStateLog(stateLog(states));
        List<TimestampedIndication> indications = result.getIndicationsMap().getIndications(2);

        assertThat(indications, hasSize(1));
        assertThat(indications.get(0).getIndication(), is(SpatSignalIndication.GREEN));
    }

    @Test
    void doesNotDeduplicateConsecutiveSameColorIndications() {
        // e.g. "protected clearance" followed by "permissive clearance" both map to YELLOW,
        // and are deliberately kept as two separate entries (see the inline comment in
        // fromSignalGroupStateLog) - lock in that behavior.
        Map<Integer, List<TimestampedState>> states = new HashMap<>();
        states.put(3, List.of(
                ts(T0, ProcessedMovementPhaseState.PROTECTED_CLEARANCE),
                ts(T0.plusSeconds(5), ProcessedMovementPhaseState.PERMISSIVE_CLEARANCE)));

        SignalGroupIndicationLog result = SignalGroupIndicationLog.fromSignalGroupStateLog(stateLog(states));
        List<TimestampedIndication> indications = result.getIndicationsMap().getIndications(3);

        assertThat(indications, hasSize(2));
        assertThat(indications.get(0).getIndication(), is(SpatSignalIndication.YELLOW));
        assertThat(indications.get(1).getIndication(), is(SpatSignalIndication.YELLOW));
    }
}
