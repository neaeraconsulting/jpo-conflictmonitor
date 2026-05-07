package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
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

    public RouteConfig findRouteConfig(final int routeId) {
        for (RouteConfig routeConfig : routes) {
            if (routeConfig.getRouteId() == routeId) return routeConfig;
        }
        throw new IllegalArgumentException(String.format("Route ID %s not found", routeId));
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
