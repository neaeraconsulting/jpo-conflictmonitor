package us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * Covers SignalGroupPhases: the signal-group -> phase-set lookup and aggregation helpers.
 */
class SignalGroupPhasesTest {

    @Test
    void phasesReturnsTheConfiguredSetForASignalGroup() {
        var map = new SignalGroupPhases();
        map.put(1, Set.of(2, 6));

        assertThat(map.phases(1), containsInAnyOrder(2, 6));
    }

    @Test
    void phasesReturnsEmptySetForAnUnconfiguredSignalGroup() {
        var map = new SignalGroupPhases();

        assertThat(map.phases(99), is(empty()));
    }

    @Test
    void phasesForASetOfSignalGroupsUnionsTheirPhaseSets() {
        var map = new SignalGroupPhases();
        map.put(1, Set.of(2));
        map.put(2, Set.of(4));

        assertThat(map.phases(Set.of(1, 2)), containsInAnyOrder(2, 4));
    }

    @Test
    void allPhasesReturnsTheUnionOfEveryConfiguredSignalGroupsPhases() {
        var map = new SignalGroupPhases();
        map.put(1, Set.of(2, 6));
        map.put(2, Set.of(4));

        assertThat(map.allPhases(), containsInAnyOrder(2, 4, 6));
    }
}
