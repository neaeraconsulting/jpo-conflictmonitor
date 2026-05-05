package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

import lombok.Data;

@Data
public class PhaseConfig {

    /**
     * Signal Group ID from the SPAT
     */
    private Integer signalGroupId;

    /**
     * ATSPM Primary Phase
     */
    private Integer primaryPhase;

    /**
     * ATSPM Secondary Phase
     */
    private Integer secondaryPhase;

    /**
     * Description of the phase/movements
     */
    private String description;

}
