package us.dot.its.jpo.conflictmonitor.batch.models.spat;

import lombok.Getter;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;

import java.util.Optional;

/**
 * SPAT signal Indications relevant for the ATSPM-SPAT validation algorithm
 * <ul>
 *  <li>RED (stop-Then-Proceed or stop-And-remain)</li>
 *  <li>GREEN (permissive-Movement-Allowed or protected-Movement-Allowed)</li>
 *  <li>YELLOW (permissive-Clearance/protected-Clearance)</li>
 *  </ul>
 *  Other SPAT states are ignored by this algorithm and not included in this enum.
 */
@Getter
public enum SpatSignalIndication {
    GREEN,
    YELLOW,
    RED;

    /**
     * Parse a SPAT Processed Movement Phase State
     * @param movementPhaseState the ProcessedSpat enum
     * @return The indication or empty for spat states that don't represent RED, GREEN or YELLOW
     */
    public static Optional<SpatSignalIndication> fromMovementPhaseState(
            ProcessedMovementPhaseState movementPhaseState) {
        if (movementPhaseState == null) return Optional.empty();
        return switch (movementPhaseState) {
            case UNAVAILABLE, DARK, PRE_MOVEMENT, CAUTION_CONFLICTING_TRAFFIC -> Optional.empty();
            case STOP_THEN_PROCEED, STOP_AND_REMAIN -> Optional.of(RED);
            case PERMISSIVE_MOVEMENT_ALLOWED, PROTECTED_MOVEMENT_ALLOWED -> Optional.of(GREEN);
            case PERMISSIVE_CLEARANCE, PROTECTED_CLEARANCE -> Optional.of(YELLOW);
        };
    }

}
