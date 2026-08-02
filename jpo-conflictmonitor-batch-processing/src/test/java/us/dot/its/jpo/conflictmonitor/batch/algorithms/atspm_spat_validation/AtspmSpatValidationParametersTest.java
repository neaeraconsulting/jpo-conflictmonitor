package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

import org.junit.jupiter.api.Test;

import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers AtspmSpatValidationParameters: route lookup and the validating unit setters.
 */
class AtspmSpatValidationParametersTest {

    private RouteConfig routeConfig(int routeId) {
        var rc = new RouteConfig();
        rc.setRouteId(routeId);
        return rc;
    }

    @Test
    void findRouteConfigReturnsTheMatchingRoute() {
        var params = new AtspmSpatValidationParameters();
        var route1 = routeConfig(1);
        var route2 = routeConfig(2);
        params.setRoutes(List.of(route1, route2));

        assertThat(params.findRouteConfig(2), is(route2));
    }

    @Test
    void findRouteConfigThrowsWhenRouteIdNotFound() {
        var params = new AtspmSpatValidationParameters();
        params.setRoutes(List.of(routeConfig(1)));

        assertThrows(IllegalArgumentException.class, () -> params.findRouteConfig(99));
    }

    @Test
    void setIntervalUnitsAcceptsHoursMinutesAndSeconds() {
        var params = new AtspmSpatValidationParameters();

        params.setIntervalUnits(ChronoUnit.HOURS);
        assertThat(params.getIntervalUnits(), is(ChronoUnit.HOURS));

        params.setIntervalUnits(ChronoUnit.MINUTES);
        assertThat(params.getIntervalUnits(), is(ChronoUnit.MINUTES));

        params.setIntervalUnits(ChronoUnit.SECONDS);
        assertThat(params.getIntervalUnits(), is(ChronoUnit.SECONDS));
    }

    @Test
    void setIntervalUnitsRejectsOtherChronoUnits() {
        var params = new AtspmSpatValidationParameters();

        assertThrows(IllegalArgumentException.class, () -> params.setIntervalUnits(ChronoUnit.DAYS));
    }

    @Test
    void setTaskStartTimeStaggerUnitsAcceptsHoursMinutesAndSeconds() {
        var params = new AtspmSpatValidationParameters();

        params.setTaskStartTimeStaggerUnits(ChronoUnit.HOURS);
        assertThat(params.getTaskStartTimeStaggerUnits(), is(ChronoUnit.HOURS));

        params.setTaskStartTimeStaggerUnits(ChronoUnit.MINUTES);
        assertThat(params.getTaskStartTimeStaggerUnits(), is(ChronoUnit.MINUTES));

        params.setTaskStartTimeStaggerUnits(ChronoUnit.SECONDS);
        assertThat(params.getTaskStartTimeStaggerUnits(), is(ChronoUnit.SECONDS));
    }

    @Test
    void setTaskStartTimeStaggerUnitsRejectsOtherChronoUnits() {
        var params = new AtspmSpatValidationParameters();

        assertThrows(IllegalArgumentException.class, () -> params.setTaskStartTimeStaggerUnits(ChronoUnit.DAYS));
    }
}
