package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import java.util.TreeMap;

public class SignalIdToPhaseEventsMap extends TreeMap<String, PhaseToEventsMap> {
    public PhaseToEventsMap getPhaseMap(String signalId) {
        if (!containsKey(signalId)) return new PhaseToEventsMap();
        return get(signalId);
    }
    public void putPhaseMap(String signalId, PhaseToEventsMap phaseMap) {
        put(signalId, phaseMap);
    }
    public void putEvent(String signalId, ProcessedControllerEvent event) {
        if (containsKey(signalId)) {
            getPhaseMap(signalId).putEvent(event.getPhase(), event);
        } else {
            PhaseToEventsMap phaseMap = new PhaseToEventsMap();
            phaseMap.putEvent(event.getPhase(), event);
            putPhaseMap(signalId, phaseMap);
        }
    }
}