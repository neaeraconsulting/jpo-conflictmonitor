package us.dot.its.jpo.conflictmonitor.atspm.models.atspm_api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class RouteSignal {

    @JsonProperty("Id")
    private int id;

    @JsonProperty("RouteId")
    private int routeId;

    @JsonProperty("Route")
    private Route route;

    @JsonProperty("Order")
    private int order;

    @JsonProperty("SignalId")
    private String signalId;

    @JsonProperty("Signal")
    private Signal signal;
}

