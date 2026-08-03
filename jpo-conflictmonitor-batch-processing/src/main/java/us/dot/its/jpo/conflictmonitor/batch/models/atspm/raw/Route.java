package us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class Route {

    @JsonProperty("Id")
    private int id;

    @JsonProperty("RouteName")
    private String routeName;

    @JsonProperty("RouteSignals")
    private List<RouteSignal> routeSignals;
}

