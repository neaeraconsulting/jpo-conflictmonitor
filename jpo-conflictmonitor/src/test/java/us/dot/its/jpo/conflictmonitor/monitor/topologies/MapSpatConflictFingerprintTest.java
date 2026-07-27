package us.dot.its.jpo.conflictmonitor.monitor.topologies;

import org.junit.Test;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class MapSpatConflictFingerprintTest {

    @Test
    public void fingerprintStableAcrossSamePhasesAndEnabledLanes() {
        Map<Integer, ProcessedMovementPhaseState> states = new HashMap<>();
        states.put(2, ProcessedMovementPhaseState.STOP_AND_REMAIN);
        states.put(4, ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED);
        Set<Integer> enabled = Set.of(1, 2);

        long a = MapSpatMessageAssessmentTopology.conflictSpatFingerprint(states, enabled);
        long b = MapSpatMessageAssessmentTopology.conflictSpatFingerprint(new HashMap<>(states), Set.of(1, 2));
        assertEquals(a, b);
    }

    @Test
    public void fingerprintChangesWhenPhaseChanges() {
        Map<Integer, ProcessedMovementPhaseState> a = Map.of(
                2, ProcessedMovementPhaseState.STOP_AND_REMAIN);
        Map<Integer, ProcessedMovementPhaseState> b = Map.of(
                2, ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED);

        assertNotEquals(
                MapSpatMessageAssessmentTopology.conflictSpatFingerprint(a, Set.of()),
                MapSpatMessageAssessmentTopology.conflictSpatFingerprint(b, Set.of()));
    }

    @Test
    public void fingerprintChangesWhenEnabledLanesChange() {
        Map<Integer, ProcessedMovementPhaseState> states = Map.of(
                2, ProcessedMovementPhaseState.STOP_AND_REMAIN);

        assertNotEquals(
                MapSpatMessageAssessmentTopology.conflictSpatFingerprint(states, Set.of(1)),
                MapSpatMessageAssessmentTopology.conflictSpatFingerprint(states, Set.of(1, 2)));
    }
}
