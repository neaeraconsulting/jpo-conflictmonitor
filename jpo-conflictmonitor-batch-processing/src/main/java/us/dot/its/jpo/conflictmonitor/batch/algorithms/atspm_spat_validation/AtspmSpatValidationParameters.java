package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.thymeleaf.util.ListUtils;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
@Component
@ConfigurationProperties(prefix = "cm.batch.atspm.spat.validation")
public class AtspmSpatValidationParameters {
    private String algorithm;
    private ZoneId localTimeZone;
    private int interval;
    /**
     * Units must be HOURS, MINUTES, or SECONDS
     */
    private ChronoUnit intervalUnits;

    /**
     * Offset to stagger the queries for each route to avoid overloading the ATSPM server
     * with simultaneous queries for multiple routes.
     */
    private int taskStartTimeStagger;
    private ChronoUnit taskStartTimeStaggerUnits;

    /**
     * Time to offset queries relative to clocktime "now" to ensure
     * all data is ingested and available for the query period
     */
    private int gracePeriodOffset;
    private ChronoUnit gracePeriodOffsetUnits;

    private List<RouteConfig> routes;

    /**
     * Map for quick lookup of routes.
     */
    @JsonIgnore
    private ImmutableMap<Integer, RouteConfig> routeIdMap = ImmutableMap.of();

    /**
     * Initialize the hashmap.
     * Assumes the list is only initialized once when the configuration is loaded.
     * No routes can be added at runtime without re-initializing the entire list.
     * @param routes List of routes loaded from configuration
     */
    public void setRoutes(List<RouteConfig> routes) {
        if (routes != null) {
            this.routes = ImmutableList.copyOf(routes);
            var mapBuilder = new ImmutableMap.Builder<Integer, RouteConfig>();
            for (RouteConfig routeConfig : routes) {
                mapBuilder.put(routeConfig.getRouteId(), routeConfig);
            }
            routeIdMap = mapBuilder.build();
        } else {
            this.routes = ImmutableList.of();
            this.routeIdMap = ImmutableMap.of();
        }
    }

    public RouteConfig findRouteConfig(final int routeId) {
        RouteConfig routeConfig = routeIdMap.get(routeId);
        if (routeConfig == null) {
            throw new IllegalArgumentException(String.format("Route ID %s not found", routeId));
        }
        return routeConfig;
    }

    @JsonIgnore
    private final static Set<ChronoUnit> validUnits = Set.of(ChronoUnit.HOURS, ChronoUnit.MINUTES, ChronoUnit.SECONDS);

    /**
     * Validates if units are HOURS, MINUTES, or SECONDS
     * @param intervalUnits Units
     */
    public void setIntervalUnits(ChronoUnit intervalUnits) {
        if (!validUnits.contains(intervalUnits)) {
            throw new IllegalArgumentException("Invalid intervalUnits: " + intervalUnits);
        }
        this.intervalUnits = intervalUnits;
    }

    /**
     * Validates if units are HOURS, MINUTES, or SECONDS
     * @param taskStartTimeStaggerUnits Units
     */
    public void setTaskStartTimeStaggerUnits(ChronoUnit taskStartTimeStaggerUnits) {
        if (!validUnits.contains(taskStartTimeStaggerUnits)) {
            throw new IllegalArgumentException("Invalid taskStartTimeStaggerUnits: " + taskStartTimeStaggerUnits);
        }
        this.taskStartTimeStaggerUnits = taskStartTimeStaggerUnits;
    }
}
