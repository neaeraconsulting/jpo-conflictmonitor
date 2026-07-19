package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import org.junit.jupiter.api.Test;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Covers SpatSignalIndication#fromMovementPhaseState, the SPaT-state -> RED/YELLOW/GREEN
 * mapping that everything downstream (indication logs, pairing) is built on.
 */
class SpatSignalIndicationTest {

    @Test
    void mapsStopStatesToRed() {
        assertThat(SpatSignalIndication.fromMovementPhaseState(ProcessedMovementPhaseState.STOP_THEN_PROCEED),
                is(Optional.of(SpatSignalIndication.RED)));
        assertThat(SpatSignalIndication.fromMovementPhaseState(ProcessedMovementPhaseState.STOP_AND_REMAIN),
                is(Optional.of(SpatSignalIndication.RED)));
    }

    @Test
    void mapsMovementAllowedStatesToGreen() {
        assertThat(SpatSignalIndication.fromMovementPhaseState(ProcessedMovementPhaseState.PERMISSIVE_MOVEMENT_ALLOWED),
                is(Optional.of(SpatSignalIndication.GREEN)));
        assertThat(SpatSignalIndication.fromMovementPhaseState(ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED),
                is(Optional.of(SpatSignalIndication.GREEN)));
    }

    @Test
    void mapsClearanceStatesToYellow() {
        assertThat(SpatSignalIndication.fromMovementPhaseState(ProcessedMovementPhaseState.PERMISSIVE_CLEARANCE),
                is(Optional.of(SpatSignalIndication.YELLOW)));
        assertThat(SpatSignalIndication.fromMovementPhaseState(ProcessedMovementPhaseState.PROTECTED_CLEARANCE),
                is(Optional.of(SpatSignalIndication.YELLOW)));
    }

    @Test
    void returnsEmptyForStatesNotRelevantToThisAlgorithm() {
        assertThat(SpatSignalIndication.fromMovementPhaseState(ProcessedMovementPhaseState.UNAVAILABLE), is(Optional.empty()));
        assertThat(SpatSignalIndication.fromMovementPhaseState(ProcessedMovementPhaseState.DARK), is(Optional.empty()));
        assertThat(SpatSignalIndication.fromMovementPhaseState(ProcessedMovementPhaseState.PRE_MOVEMENT), is(Optional.empty()));
        assertThat(SpatSignalIndication.fromMovementPhaseState(ProcessedMovementPhaseState.CAUTION_CONFLICTING_TRAFFIC), is(Optional.empty()));
    }

    @Test
    void returnsEmptyForNullMovementPhaseState() {
        assertThat(SpatSignalIndication.fromMovementPhaseState(null), is(Optional.empty()));
    }
}
