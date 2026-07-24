package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Covers PhaseToEventsMap#findEventInWindow, the nearest-event matching logic that
 * AtspmSpatValidationServiceImpl relies on to pair SPaT indications with ATSPM events.
 */
class PhaseToEventsMapTest {

    private static final Instant BASE = Instant.parse("2026-05-03T10:00:00Z");

    private ProcessedControllerEvent event(Instant timestamp, EventCode code) {
        var e = new ProcessedControllerEvent();
        e.setTimestamp(timestamp);
        e.setEventCode(code);
        e.setPhase(2);
        return e;
    }

    @Test
    void findsNearestEventWithinWindowAndReportsPaired() {
        var map = new PhaseToEventsMap();
        map.putEvent(2, event(BASE, EventCode.GREEN));
        map.putEvent(2, event(BASE.plusSeconds(10), EventCode.GREEN));
        map.putEvent(2, event(BASE.plusSeconds(1), EventCode.GREEN));

        var result = map.findEventInWindow(2, EventCode.GREEN, BASE.plusSeconds(2), Duration.ofSeconds(3));

        assertThat(result.paired(), is(true));
        assertThat(result.event(), is(notNullValue()));
        assertThat(result.event().getTimestamp(), is(BASE.plusSeconds(1)));
    }

    @Test
    void returnsNearestButUnpairedWhenClosestEventIsOutsideWindow() {
        var map = new PhaseToEventsMap();
        map.putEvent(2, event(BASE, EventCode.RED));

        var result = map.findEventInWindow(2, EventCode.RED, BASE.plusSeconds(10), Duration.ofSeconds(3));

        assertThat(result.paired(), is(false));
        assertThat(result.event(), is(notNullValue()));
        assertThat(result.event().getTimestamp(), is(BASE));
    }

    @Test
    void returnsUnpairedWhenPhaseHasNoEvents() {
        var map = new PhaseToEventsMap();

        var result = map.findEventInWindow(2, EventCode.GREEN, BASE, Duration.ofSeconds(3));

        assertThat(result.paired(), is(false));
        assertThat(result.event(), is(nullValue()));
    }

    @Test
    void returnsUnpairedWhenPhaseHasEventsButNoneMatchEventCode() {
        var map = new PhaseToEventsMap();
        map.putEvent(2, event(BASE, EventCode.GREEN));

        var result = map.findEventInWindow(2, EventCode.RED, BASE, Duration.ofSeconds(3));

        assertThat(result.paired(), is(false));
        assertThat(result.event(), is(nullValue()));
    }

    @Test
    void picksNearestEventRegardlessOfListOrdering() {
        var map = new PhaseToEventsMap();
        // far candidate added first, near candidate added second - list is not time-sorted
        map.putEvent(2, event(BASE.plusSeconds(100), EventCode.YELLOW));
        map.putEvent(2, event(BASE, EventCode.YELLOW));

        var result = map.findEventInWindow(2, EventCode.YELLOW, BASE.plusSeconds(1), Duration.ofSeconds(3));

        assertThat(result.paired(), is(true));
        assertThat(result.event().getTimestamp(), is(BASE));
    }
}
