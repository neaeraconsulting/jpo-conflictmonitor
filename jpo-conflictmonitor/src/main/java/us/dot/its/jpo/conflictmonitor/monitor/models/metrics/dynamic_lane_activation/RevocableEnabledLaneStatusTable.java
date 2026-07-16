package us.dot.its.jpo.conflictmonitor.monitor.models.metrics.dynamic_lane_activation;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.*;

/**
 * Table of the status of each revocable lane at an intersection over time.
 * Serialized as a List, keeps track of lane IDs with an internal Map.
 */
@Data
@EqualsAndHashCode(callSuper=true)
public class RevocableEnabledLaneStatusTable extends AbstractList<RevocableEnabledLaneStatusChanges> {

    public RevocableEnabledLaneStatusTable() {
        super();
    }

    public RevocableEnabledLaneStatusTable(Collection<RevocableEnabledLaneStatusChanges> coll) {
        this.addAll(coll);
    }

    private final List<RevocableEnabledLaneStatusChanges> list = new ArrayList<>();
    private final Map<Integer, RevocableEnabledLaneStatusChanges> laneIdMap = new HashMap<>();

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public RevocableEnabledLaneStatusChanges get(int index) {
        return list.get(index);
    }

    @Override
    public RevocableEnabledLaneStatusChanges set(int index, RevocableEnabledLaneStatusChanges statusChanges) {
        RevocableEnabledLaneStatusChanges previous = list.get(index);
        list.set(index, statusChanges);
        laneIdMap.put(statusChanges.getLaneID(), statusChanges);
        // Just in case the list gets sorted and index changes, prevent keeping stale entry
        if (previous.getLaneID() != statusChanges.getLaneID() && !laneIdMap.containsValue(previous)) {
            laneIdMap.remove(previous.getLaneID());
        }
        return previous;
    }

    @Override
    public void add(int index, RevocableEnabledLaneStatusChanges statusChanges) {
        list.add(index, statusChanges);
        laneIdMap.put(statusChanges.getLaneID(), statusChanges);
    }

    @Override
    public RevocableEnabledLaneStatusChanges remove(int index) {
        RevocableEnabledLaneStatusChanges previous = list.remove(index);
        if (previous != null) {
            int laneId = previous.getLaneID();
            laneIdMap.remove(laneId);
        }
        return previous;
    }


    public RevocableEnabledLaneStatusChanges getChangesForLaneID(final int laneID) {
        return laneIdMap.get(laneID);
    }


}
