package us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat;

import lombok.Data;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.EventCode;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatSignalIndication;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;

import java.time.Instant;

@Data
public class AtspmSpatPair {

    // ATSPM properties may be null if unpaired
    private Instant atspmTimestamp;
    private Integer atspmPrimaryPhase;
    /**
     * Optional
     */
    private Integer atspmSecondaryPhase;
    /**
     * ATSPM Event: RED, GREEN, or YELLOW
     */
    private EventCode atspmEventCode;


    // SPAT properties
    private Instant spatTimestamp;
    private Integer spatSignalGroupId;
    /**
     * SPAT Movement Phase State:
     */
    private ProcessedMovementPhaseState spatMovementPhaseState;
    /**
     * SPAT signal indication: RED, GREEN, or YELLOW
     */
    private SpatSignalIndication spatIndication;

    /**
     * Paired flag is set if corresponding signal indication event for ATSPM
     * and SPAT data occur within 3 seconds.
     */
    private boolean isPaired;

}
