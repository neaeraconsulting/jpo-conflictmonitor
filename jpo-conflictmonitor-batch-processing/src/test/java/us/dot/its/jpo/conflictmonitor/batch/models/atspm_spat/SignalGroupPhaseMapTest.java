package us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat;

import org.junit.jupiter.api.Test;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.PhaseConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.SignalConfig;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Covers SignalGroupPhaseMap: the signal-group-id <-> ATSPM primary/secondary phase
 * mapping derived from SignalConfig.
 */
class SignalGroupPhaseMapTest {

    private PhaseConfig phaseConfig(int signalGroupId, Integer primaryPhase, Integer secondaryPhase) {
        var pc = new PhaseConfig();
        pc.setSignalGroupId(signalGroupId);
        pc.setPrimaryPhase(primaryPhase);
        pc.setSecondaryPhase(secondaryPhase);
        return pc;
    }

    @Test
    void phasesReturnsBothPrimaryAndSecondaryWhenBothConfigured() {
        var map = new SignalGroupPhaseMap();
        map.put(1, phaseConfig(1, 2, 6));

        assertThat(map.phases(1), containsInAnyOrder(2, 6));
    }

    @Test
    void phasesReturnsOnlyPrimaryWhenNoSecondaryConfigured() {
        var map = new SignalGroupPhaseMap();
        map.put(1, phaseConfig(1, 2, null));

        assertThat(map.phases(1), contains(2));
    }

    @Test
    void phasesReturnsEmptySetForUnconfiguredSignalGroup() {
        var map = new SignalGroupPhaseMap();

        assertThat(map.phases(99), is(empty()));
    }

    @Test
    void getPhaseConfigDefaultsPrimaryPhaseToTheSignalGroupsOwnNumberWhenUnconfigured() {
        var map = new SignalGroupPhaseMap();

        PhaseConfig defaultConfig = map.getPhaseConfig(7);

        assertThat(defaultConfig.getPrimaryPhase(), is(7));
        assertThat(defaultConfig.getSecondaryPhase(), is(nullValue()));
    }

    @Test
    void getPhaseConfigReturnsTheConfiguredEntryWhenPresent() {
        var map = new SignalGroupPhaseMap();
        map.put(1, phaseConfig(1, 2, 6));

        assertThat(map.getPhaseConfig(1).getPrimaryPhase(), is(2));
    }

    @Test
    void primaryPhasesForSecondaryFindsAllPhaseConfigsSharingThatSecondaryPhase() {
        var map = new SignalGroupPhaseMap();
        map.put(1, phaseConfig(1, 2, 6));
        map.put(5, phaseConfig(5, 4, 6));

        assertThat(map.primaryPhasesForSecondary(6), containsInAnyOrder(2, 4));
    }

    @Test
    void primaryPhasesForSecondaryReturnsEmptyWhenPhaseIsNeverASecondary() {
        var map = new SignalGroupPhaseMap();
        map.put(1, phaseConfig(1, 2, 6));

        assertThat(map.primaryPhasesForSecondary(99), is(empty()));
    }

    @Test
    void fromSignalConfigBuildsMapKeyedBySignalGroupId() {
        var signalConfig = new SignalConfig();
        signalConfig.setSignalId("SIG1");
        signalConfig.setPhases(List.of(phaseConfig(3, 3, null)));

        SignalGroupPhaseMap map = SignalGroupPhaseMap.fromSignalConfig(signalConfig);

        assertThat(map.keySet(), contains(3));
        assertThat(map.getPhaseConfig(3).getPrimaryPhase(), is(3));
    }

    @Test
    void fromSignalConfigReturnsEmptyMapForNullConfigOrNullPhasesList() {
        assertThat(SignalGroupPhaseMap.fromSignalConfig(null).isEmpty(), is(true));

        var signalConfigWithNoPhases = new SignalConfig();
        assertThat(SignalGroupPhaseMap.fromSignalConfig(signalConfigWithNoPhases).isEmpty(), is(true));
    }
}
