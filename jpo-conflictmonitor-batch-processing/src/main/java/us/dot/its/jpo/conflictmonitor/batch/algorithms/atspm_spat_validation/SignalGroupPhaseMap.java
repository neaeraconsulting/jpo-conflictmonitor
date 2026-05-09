package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

import java.util.Map;
import java.util.TreeMap;

public class SignalGroupPhaseMap extends TreeMap<Integer, PhaseConfig> {
    public SignalGroupPhaseMap() {
        super();
    }
    public SignalGroupPhaseMap(Map<Integer, PhaseConfig> map) {
        super(map);
    }
}
