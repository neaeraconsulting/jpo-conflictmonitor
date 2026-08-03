package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import org.junit.jupiter.api.Test;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.PhaseConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.RouteConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.SignalConfig;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.ControllerEventLog;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Covers ProcessedControllerEventLog's constructor logic: grouping raw ControllerEventLog
 * entries by signal/phase, sorting, and merging secondary-phase events into the primary
 * phase's event list.
 */
class ProcessedControllerEventLogTest {

    private static final String SIGNAL_ID = "SIG1";
    private static final Instant START = Instant.parse("2026-05-03T09:00:00Z");
    private static final Instant END = Instant.parse("2026-05-03T11:00:00Z");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-05-03T09:00:00Z"), ZoneOffset.UTC);

    private static final int GREEN = 1;
    private static final int YELLOW = 8;
    private static final int RED = 10;

    private ControllerEventLog rawEvent(String time, int eventCode, int phase) {
        var e = new ControllerEventLog();
        e.setSignalId(SIGNAL_ID);
        e.setTimestamp(time);
        e.setEventCode(eventCode);
        e.setEventParam(phase);
        return e;
    }

    private RouteConfig routeConfig(Integer secondaryPhase) {
        var phaseConfig = new PhaseConfig();
        phaseConfig.setSignalGroupId(1);
        phaseConfig.setPrimaryPhase(2);
        phaseConfig.setSecondaryPhase(secondaryPhase);

        var signalConfig = new SignalConfig();
        signalConfig.setSignalId(SIGNAL_ID);
        signalConfig.setIntersectionId(100);
        signalConfig.setEnabled(true);
        signalConfig.setPhases(List.of(phaseConfig));

        var routeConfig = new RouteConfig();
        routeConfig.setRouteId(1);
        routeConfig.setSignals(List.of(signalConfig));
        return routeConfig;
    }

    private ProcessedControllerEventLog build(RouteConfig routeConfig, ControllerEventLog... events) {
        return new ProcessedControllerEventLog(1, START, END, List.of(events), CLOCK, ZoneOffset.UTC, routeConfig);
    }

    @Test
    void mergePrefersPrimaryEventsWhenPrimaryIsNotRed() {
        RouteConfig routeConfig = routeConfig(6);
        ProcessedControllerEventLog log = build(routeConfig,
                rawEvent("2026-05-03T10:00:00", GREEN, 2),
                rawEvent("2026-05-03T10:00:05", GREEN, 6), // secondary event - should be ignored
                rawEvent("2026-05-03T10:00:10", YELLOW, 2));

        List<ProcessedControllerEvent> phase2Events = log.getSignalPhaseMap().getPhaseMap(SIGNAL_ID).getEventList(2);

        assertThat(phase2Events, hasSize(2));
        assertThat(phase2Events.getFirst().getEventCode(), is(EventCode.GREEN));
        assertThat(phase2Events.getFirst().getPhase(), is(2));
        assertThat(phase2Events.getFirst().getSecondaryPhase(), is(nullValue()));
        assertThat(phase2Events.get(1).getEventCode(), is(EventCode.YELLOW));
        assertThat(phase2Events.get(1).getPhase(), is(2));
    }

    @Test
    void mergeUsesSecondaryEventWhenPrimaryIsRed() {
        RouteConfig routeConfig = routeConfig(6);
        ProcessedControllerEventLog log = build(routeConfig,
                rawEvent("2026-05-03T10:00:00", GREEN, 2),
                rawEvent("2026-05-03T10:00:15", GREEN, 6), // most recent secondary before RED
                rawEvent("2026-05-03T10:00:20", RED, 2));

        List<ProcessedControllerEvent> phase2Events = log.getSignalPhaseMap().getPhaseMap(SIGNAL_ID).getEventList(2);

        assertThat(phase2Events, hasSize(2));
        assertThat(phase2Events.getFirst().getEventCode(), is(EventCode.GREEN));
        assertThat(phase2Events.getFirst().getTimestamp(), is(Instant.parse("2026-05-03T10:00:00Z")));

        ProcessedControllerEvent merged = phase2Events.get(1);
        assertThat(merged.getEventCode(), is(EventCode.GREEN));
        // Timestamped at the primary's own red-transition time, not the secondary
        // transition's earlier timestamp, since this entry represents the inferred
        // indication as of when the primary went red.
        assertThat(merged.getTimestamp(), is(Instant.parse("2026-05-03T10:00:20Z")));
        assertThat(merged.getPhase(), is(2));
        assertThat(merged.getSecondaryPhase(), is(6));
    }

    @Test
    void mergeFallsBackToPrimaryRedEventWhenNoSecondaryEventSeenYet() {
        RouteConfig routeConfig = routeConfig(6);
        // No secondary (phase 6) events at all
        ProcessedControllerEventLog log = build(routeConfig,
                rawEvent("2026-05-03T10:00:00", RED, 2));

        List<ProcessedControllerEvent> phase2Events = log.getSignalPhaseMap().getPhaseMap(SIGNAL_ID).getEventList(2);

        assertThat(phase2Events, hasSize(1));
        assertThat(phase2Events.getFirst().getEventCode(), is(EventCode.RED));
        assertThat(phase2Events.getFirst().getPhase(), is(2));
        assertThat(phase2Events.getFirst().getSecondaryPhase(), is(nullValue()));
    }

    @Test
    void mergedEventListStaysSortedByTimestamp() {
        // A primary RED event merges with a stale secondary event whose timestamp is
        // earlier than an intervening non-red primary event already added to the list.
        RouteConfig routeConfig = routeConfig(6);
        ProcessedControllerEventLog log = build(routeConfig,
                rawEvent("2026-05-03T10:00:05", GREEN, 6),  // stale secondary
                rawEvent("2026-05-03T10:00:10", YELLOW, 2), // non-red primary, added as-is
                rawEvent("2026-05-03T10:00:20", RED, 2));   // merges with the stale secondary

        List<ProcessedControllerEvent> phase2Events = log.getSignalPhaseMap().getPhaseMap(SIGNAL_ID).getEventList(2);

        assertThat(phase2Events, hasSize(2));
        assertThat("merged phase event list should stay non-decreasing by timestamp",
                phase2Events.getFirst().getTimestamp(), lessThanOrEqualTo(phase2Events.get(1).getTimestamp()));
    }

    @Test
    void signalsWithoutASecondaryPhaseConfiguredAreUnaffectedByMerge() {
        RouteConfig routeConfig = routeConfig(null);
        ProcessedControllerEventLog log = build(routeConfig,
                rawEvent("2026-05-03T10:00:00", GREEN, 2),
                rawEvent("2026-05-03T10:00:05", GREEN, 6), // unrelated phase - not anyone's configured secondary
                rawEvent("2026-05-03T10:00:10", RED, 2));

        PhaseToEventsMap phaseMap = log.getSignalPhaseMap().getPhaseMap(SIGNAL_ID);

        assertThat(phaseMap.getEventList(2), hasSize(2));
        assertThat(phaseMap.getEventList(2).getFirst().getSecondaryPhase(), is(nullValue()));
        assertThat(phaseMap.getEventList(2).get(1).getSecondaryPhase(), is(nullValue()));
        // the unrelated phase 6 event is untouched, still present under its own key
        assertThat(phaseMap.getEventList(6), hasSize(1));
    }

    @Test
    void ignoresEventsWithEventCodesNotRelevantToThisAlgorithm() {
        RouteConfig routeConfig = routeConfig(null);
        ProcessedControllerEventLog log = build(routeConfig,
                rawEvent("2026-05-03T10:00:00", GREEN, 2),
                rawEvent("2026-05-03T10:00:05", 43, 2), // e.g. a detector actuation code - not GREEN/YELLOW/RED
                rawEvent("2026-05-03T10:00:10", RED, 2));

        List<ProcessedControllerEvent> phase2Events = log.getSignalPhaseMap().getPhaseMap(SIGNAL_ID).getEventList(2);

        assertThat(phase2Events, hasSize(2));
        assertThat(phase2Events.getFirst().getEventCode(), is(EventCode.GREEN));
        assertThat(phase2Events.get(1).getEventCode(), is(EventCode.RED));
    }

    @Test
    void sizeCountsAllEventsAcrossAllSignalsAndPhases() {
        RouteConfig routeConfig = routeConfig(null);
        ProcessedControllerEventLog log = build(routeConfig,
                rawEvent("2026-05-03T10:00:00", GREEN, 2),
                rawEvent("2026-05-03T10:00:05", YELLOW, 2),
                rawEvent("2026-05-03T10:00:10", RED, 2));

        assertThat(log.size(), is(3L));
    }

    @Test
    void signalToPhaseMultimapReflectsPhaseKeysAfterMerging() {
        RouteConfig routeConfig = routeConfig(6);
        ProcessedControllerEventLog log = build(routeConfig,
                rawEvent("2026-05-03T10:00:00", GREEN, 2),
                rawEvent("2026-05-03T10:00:15", GREEN, 6),
                rawEvent("2026-05-03T10:00:20", RED, 2));

        var multimap = log.signalToPhaseMultimap();

        // primary phase 2 is present (from the merged list); the raw secondary phase 6 key is
        // left untouched by merge() and so its events also still show up under phase 6.
        assertThat(multimap.get(SIGNAL_ID), hasItems(2, 6));
    }
}
