package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class SignalConfig {
    /**
     * ATSPM Signal ID
     */
    private String signalId;

    /**
     * SPAT Intersection ID
     */
    private Integer intersectionId;

    /**
     * Description of the intersection
     */
    private String description;

    /**
     * Whether ATSPM and SPAT data are enabled for the intersection
     */
    private boolean enabled;

    private List<PhaseConfig> phases;


}
