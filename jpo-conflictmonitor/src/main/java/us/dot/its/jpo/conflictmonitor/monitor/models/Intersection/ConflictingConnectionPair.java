package us.dot.its.jpo.conflictmonitor.monitor.models.Intersection;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * A pair of geometrically crossing lane connections used for signal-state conflict checks.
 * Precomputed from MAP geometry so SPaT processing only evaluates current phase states.
 */
@Getter
@AllArgsConstructor
public class ConflictingConnectionPair {

    private final int firstIngressLaneId;
    private final int firstEgressLaneId;
    private final int firstSignalGroup;
    private final int secondIngressLaneId;
    private final int secondEgressLaneId;
    private final int secondSignalGroup;

    public boolean involvesLane(int laneId) {
        return firstIngressLaneId == laneId
                || firstEgressLaneId == laneId
                || secondIngressLaneId == laneId
                || secondEgressLaneId == laneId;
    }
}
