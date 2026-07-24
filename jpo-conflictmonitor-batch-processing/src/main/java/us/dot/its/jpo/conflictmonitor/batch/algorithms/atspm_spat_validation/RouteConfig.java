package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.List;

@Data
public class RouteConfig {
    private int routeId;
    private String description;
    private List<SignalConfig> signals;
    @JsonIgnore
    public boolean enabledSignals() {
        return signals != null && !signals.isEmpty() && signals.stream().anyMatch(SignalConfig::isEnabled);
    }

    public SignalConfig signalConfig(String signalId) {
        if (signals == null) return null;
        return signals.stream().filter(signalConfig -> signalId.equals(signalConfig.getSignalId())).findFirst().orElse(null);
    }

}
