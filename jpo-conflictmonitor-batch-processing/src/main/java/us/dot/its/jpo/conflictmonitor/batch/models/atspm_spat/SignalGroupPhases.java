package us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class SignalGroupPhases extends TreeMap<Integer, Set<Integer>> {
    public SignalGroupPhases() {
        super();
    }
    public SignalGroupPhases(Map<Integer, Set<Integer>> map) {
        super(map);
    }
    public Set<Integer> phases(int signalGroup) {
        if (!containsKey(signalGroup)) return Set.of();
        return get(signalGroup);
    }
    public Set<Integer> phases(Set<Integer> signalGroups) {
        Set<Integer> phases = new TreeSet<>();
        for (Integer signalGroup : signalGroups) {
            phases.addAll(phases(signalGroup));
        }
        return phases;
    }
    public Set<Integer> allPhases() {
        Set<Integer> allPhases = new TreeSet<>();
        for (var value : values()) {
            allPhases.addAll(value);
        }
        return allPhases;
    }
}
