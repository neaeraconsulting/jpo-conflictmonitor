package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    public SignalGroupPhaseMap signalGroupPhaseMap() {
        var map = new SignalGroupPhaseMap();
        if (phases == null) return map;
        Map<Integer, PhaseConfig> sgPhaseMap = phases.stream()
                .collect(Collectors.toUnmodifiableMap(PhaseConfig::getSignalGroupId, sg -> sg));
        return new SignalGroupPhaseMap(sgPhaseMap);
    }
}
