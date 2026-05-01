package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

import lombok.Data;

import java.util.List;

@Data
public class RouteConfig {
    private int routeId;
    private List<SignalConfig> signals;
}
