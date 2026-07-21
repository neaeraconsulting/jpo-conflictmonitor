package us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat;

import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.PhaseConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.SignalConfig;

import java.util.*;
import java.util.stream.Collectors;

/***
 * Maps SPAT Signal Group ID to ATSPM primary and secondary phase number for one intersection/signal
 */
public class SignalGroupPhaseMap extends TreeMap<Integer, PhaseConfig> {
    public SignalGroupPhaseMap() {
        super();
    }
    public SignalGroupPhaseMap(Map<Integer, PhaseConfig> map) {
        super(map);
    }

    public PhaseConfig getPhaseConfig(int signalGroup) {
        PhaseConfig configured = get(signalGroup);
        if (configured != null) {
            return configured;
        }
        // Per spec: absent an explicit override, a SPaT signal group is paired by default with
        // the ATSPM phase of the same number - an override entry is only needed when that
        // assumption doesn't hold (mismatched numbering, or a secondary/overlap phase).
        PhaseConfig defaultConfig = new PhaseConfig();
        defaultConfig.setSignalGroupId(signalGroup);
        defaultConfig.setPrimaryPhase(signalGroup);
        return defaultConfig;
    }

    /**
     * Get set of 1 or 2 phases for the signal group
     * @param signalGroup Signal group ID
     * @return 1 or 2 numbers
     */
    public Set<Integer> phases(Integer signalGroup) {
        Set<Integer> phaseSet = new TreeSet<>();
        if (containsKey(signalGroup)) {
            PhaseConfig pc = get(signalGroup);
            if (pc.getPrimaryPhase() != null) {
                phaseSet.add(pc.getPrimaryPhase());
            }
            if (pc.getSecondaryPhase() != null) {
                phaseSet.add(pc.getSecondaryPhase());
            }
        }
        return phaseSet;
    }

    public SignalGroupPhases phases() {
        var phases = this.keySet().stream().collect(
                Collectors.toUnmodifiableMap(
                        signalGroup -> signalGroup,
                        signalGroup -> phases(signalGroup)));
        return new SignalGroupPhases(phases);
    }

    public Set<Integer> primaryPhasesForSecondary(final int secondaryPhase) {
        return values().stream()
                // PhaseConfigs for which this is a secondary phase and which have a primary phase
                .filter(phaseConfig -> phaseConfig.getSecondaryPhase() != null
                        && phaseConfig.getPrimaryPhase() != null
                        && phaseConfig.getSecondaryPhase().equals(secondaryPhase))
                // Get the primary phases
                .map(PhaseConfig::getPrimaryPhase)
                .collect(Collectors.toUnmodifiableSet());
    }

    public static SignalGroupPhaseMap fromSignalConfig(SignalConfig signalConfig) {
        if (signalConfig == null || signalConfig.getPhases() == null) return new SignalGroupPhaseMap();
        Map<Integer, PhaseConfig> sgPhaseMap = signalConfig.getPhases().stream()
                .collect(Collectors.toUnmodifiableMap(PhaseConfig::getSignalGroupId, sg -> sg));
        return new SignalGroupPhaseMap(sgPhaseMap);
    }


}
