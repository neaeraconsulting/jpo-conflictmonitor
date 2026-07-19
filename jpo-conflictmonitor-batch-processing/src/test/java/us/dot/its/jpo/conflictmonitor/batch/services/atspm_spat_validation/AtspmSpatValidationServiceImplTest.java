package us.dot.its.jpo.conflictmonitor.batch.services.atspm_spat_validation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.PhaseConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.RouteConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.SignalConfig;
import us.dot.its.jpo.conflictmonitor.batch.events.AtspmSpatSignalGroupAlignmentEvent;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.ControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPair;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupIndicationLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatSignalIndication;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.TimestampedIndication;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.batch.services.spat.ProcessedSpatService;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Covers AtspmSpatValidationServiceImpl: the top-level pairing (atpsmSpatLogs) and
 * signal-group-alignment (atspmSpatSignalGroupAlignmentEvents) logic, with ATSPM and
 * SPaT data sources mocked.
 */
@ExtendWith(MockitoExtension.class)
class AtspmSpatValidationServiceImplTest {

    private static final Instant START = Instant.parse("2026-05-03T09:00:00Z");
    private static final Instant END = Instant.parse("2026-05-03T11:00:00Z");
    private static final Clock CLOCK = Clock.fixed(START, ZoneOffset.UTC);

    private static final int GREEN = 1;

    @Mock
    private AtspmSpatValidationParameters parameters;
    @Mock
    private AtspmClientService atspmClientService;
    @Mock
    private ProcessedSpatService spatService;

    private AtspmSpatValidationServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new AtspmSpatValidationServiceImpl(parameters, atspmClientService, spatService);
    }

    private SignalConfig signalConfig(String signalId, Integer intersectionId, boolean enabled, PhaseConfig... phases) {
        var sc = new SignalConfig();
        sc.setSignalId(signalId);
        sc.setIntersectionId(intersectionId);
        sc.setEnabled(enabled);
        sc.setPhases(phases.length == 0 ? null : List.of(phases));
        return sc;
    }

    private PhaseConfig phaseConfig(int signalGroupId, Integer primaryPhase, Integer secondaryPhase) {
        var pc = new PhaseConfig();
        pc.setSignalGroupId(signalGroupId);
        pc.setPrimaryPhase(primaryPhase);
        pc.setSecondaryPhase(secondaryPhase);
        return pc;
    }

    private RouteConfig routeConfig(int routeId, SignalConfig... signals) {
        var rc = new RouteConfig();
        rc.setRouteId(routeId);
        rc.setSignals(List.of(signals));
        return rc;
    }

    private ControllerEventLog rawEvent(String signalId, String time, int eventCode, int phase) {
        var e = new ControllerEventLog();
        e.setSignalId(signalId);
        e.setTimestamp(time);
        e.setEventCode(eventCode);
        e.setEventParam(phase);
        return e;
    }

    private ProcessedControllerEventLog processedEventLog(RouteConfig routeConfig, ControllerEventLog... events) {
        return new ProcessedControllerEventLog(routeConfig.getRouteId(), START, END, List.of(events), CLOCK, ZoneOffset.UTC, routeConfig);
    }

    private SignalGroupIndicationLog indicationLog(int intersectionId, int signalGroup, Instant timestamp, SpatSignalIndication indication) {
        var log = new SignalGroupIndicationLog();
        log.setIntersectionId(intersectionId);
        var map = new SignalGroupIndicationLog.SignalGroupIndicationMap();
        var ti = new TimestampedIndication();
        ti.setTimestamp(timestamp);
        ti.setIndication(indication);
        map.putIndication(signalGroup, ti);
        log.setIndicationsMap(map);
        return log;
    }

    private SignalGroupIndicationLog emptyIndicationLog(int intersectionId) {
        var log = new SignalGroupIndicationLog();
        log.setIntersectionId(intersectionId);
        log.setIndicationsMap(new SignalGroupIndicationLog.SignalGroupIndicationMap());
        return log;
    }

    @Test
    void atpsmSpatLogsReturnsEmptyWhenRouteHasNoEnabledSignals() {
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", 100, false));
        when(parameters.findRouteConfig(1)).thenReturn(routeConfig);

        List<AtspmSpatPairLog> result = service.atpsmSpatLogs(1, START, END);

        assertThat(result, is(empty()));
    }

    @Test
    void atpsmSpatLogsRecordsErrorWhenSignalIsMissingIntersectionId() {
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", null, true));
        when(parameters.findRouteConfig(1)).thenReturn(routeConfig);
        when(parameters.getLocalTimeZone()).thenReturn(ZoneOffset.UTC);
        when(atspmClientService.processedEventLogs(any(), any(), eq(1)))
                .thenReturn(processedEventLog(routeConfig));

        List<AtspmSpatPairLog> result = service.atpsmSpatLogs(1, START, END);

        assertThat(result, hasSize(1));
        assertThat(result.get(0).getError(), containsString("Missing intersection id"));
    }

    @Test
    void atpsmSpatLogsRecordsErrorWhenAtspmHasNoDataForSignal() {
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", 100, true));
        when(parameters.findRouteConfig(1)).thenReturn(routeConfig);
        when(parameters.getLocalTimeZone()).thenReturn(ZoneOffset.UTC);
        // no raw events at all - "SIG1" never appears in the processed log's signal/phase map
        when(atspmClientService.processedEventLogs(any(), any(), eq(1)))
                .thenReturn(processedEventLog(routeConfig));
        when(spatService.signalGroupIndicationLogs(eq(100), any(), any()))
                .thenReturn(emptyIndicationLog(100));

        List<AtspmSpatPairLog> result = service.atpsmSpatLogs(1, START, END);

        assertThat(result, hasSize(1));
        assertThat(result.get(0).getError(), containsString("no entries for signalId"));
    }

    @Test
    void atpsmSpatLogsSkipsSignalGroupsWithNoCommonMappedPhase() {
        RouteConfig routeConfig = routeConfig(1,
                signalConfig("SIG1", 100, true, phaseConfig(1, 1, null)));
        when(parameters.findRouteConfig(1)).thenReturn(routeConfig);
        when(parameters.getLocalTimeZone()).thenReturn(ZoneOffset.UTC);
        // ATSPM data only has phase 99, which is disjoint from the mapped phase (1)
        when(atspmClientService.processedEventLogs(any(), any(), eq(1)))
                .thenReturn(processedEventLog(routeConfig, rawEvent("SIG1", "2026-05-03T10:00:00", GREEN, 99)));
        when(spatService.signalGroupIndicationLogs(eq(100), any(), any()))
                .thenReturn(indicationLog(100, 1, Instant.parse("2026-05-03T10:00:00Z"), SpatSignalIndication.GREEN));

        List<AtspmSpatPairLog> result = service.atpsmSpatLogs(1, START, END);

        assertThat(result, hasSize(1));
        assertThat(result.get(0).getError(), is(nullValue()));
        assertThat(result.get(0).getAtspmSpatPairs(), is(empty()));
    }

    @Test
    void atpsmSpatLogsPairsEachSpatIndicationWithNearestMatchingAtspmEvent() {
        // signalGroupId (1) intentionally differs from primaryPhase (5), to exercise the
        // configured mapping rather than relying on them numerically coinciding.
        //
        // NOTE: this currently fails. AtspmSpatValidationServiceImpl#atpsmSpatLogs uses the
        // raw SPaT signalGroupId directly as if it were the ATSPM phase number (see
        // commonSignalGroupPhaseNumbers.contains(signalGroup) and
        // phaseMap.findEventInWindow(signalGroup, ...)), instead of mapping through the
        // configured primaryPhase. It only happens to work when signalGroupId == primaryPhase
        // numerically, which is not guaranteed by anything in the config model.
        RouteConfig routeConfig = routeConfig(1,
                signalConfig("SIG1", 100, true, phaseConfig(1, 5, null)));
        when(parameters.findRouteConfig(1)).thenReturn(routeConfig);
        when(parameters.getLocalTimeZone()).thenReturn(ZoneOffset.UTC);
        when(atspmClientService.processedEventLogs(any(), any(), eq(1)))
                .thenReturn(processedEventLog(routeConfig, rawEvent("SIG1", "2026-05-03T10:00:00", GREEN, 5)));
        when(spatService.signalGroupIndicationLogs(eq(100), any(), any()))
                .thenReturn(indicationLog(100, 1, Instant.parse("2026-05-03T10:00:01Z"), SpatSignalIndication.GREEN));

        List<AtspmSpatPairLog> result = service.atpsmSpatLogs(1, START, END);

        assertThat(result, hasSize(1));
        AtspmSpatPairLog pairLog = result.get(0);
        assertThat(pairLog.getError(), is(nullValue()));
        assertThat(pairLog.getAtspmSpatPairs(), hasSize(1));

        AtspmSpatPair pair = pairLog.getAtspmSpatPairs().get(0);
        assertThat(pair.isPaired(), is(true));
        assertThat(pair.getSpatSignalGroupId(), is(1));
        assertThat(pair.getAtspmPrimaryPhase(), is(5));
    }

    @Test
    void atspmSpatSignalGroupAlignmentEventsFlagsMismatchedSignalGroupAndPhaseSets() {
        RouteConfig routeConfig = routeConfig(1,
                signalConfig("SIG1", 100, true, phaseConfig(1, 2, null)));
        when(parameters.findRouteConfig(1)).thenReturn(routeConfig);
        when(parameters.getLocalTimeZone()).thenReturn(ZoneOffset.UTC);
        // ATSPM data has phase 5, but the SPaT signal group maps to phase 2
        when(atspmClientService.processedEventLogs(any(), any(), eq(1)))
                .thenReturn(processedEventLog(routeConfig, rawEvent("SIG1", "2026-05-03T10:00:00", GREEN, 5)));
        when(spatService.signalGroupIndicationLogs(eq(100), any(), any()))
                .thenReturn(indicationLog(100, 1, Instant.parse("2026-05-03T10:00:00Z"), SpatSignalIndication.GREEN));

        List<AtspmSpatSignalGroupAlignmentEvent> events = service.atspmSpatSignalGroupAlignmentEvents(1, START, END);

        assertThat(events, hasSize(1));
        AtspmSpatSignalGroupAlignmentEvent event = events.get(0);
        assertThat(event.getSignalId(), is("SIG1"));
        assertThat(event.getSpatSignalGroupIds(), contains(1));
        assertThat(event.getMappedPhasesFromSpats(), contains(2));
        assertThat(event.getAtspmPhases(), contains(5));
    }

    @Test
    void atspmSpatSignalGroupAlignmentEventsSkipsSignalsWhereSetsAlign() {
        RouteConfig routeConfig = routeConfig(1,
                signalConfig("SIG1", 100, true, phaseConfig(1, 2, null)));
        when(parameters.findRouteConfig(1)).thenReturn(routeConfig);
        when(parameters.getLocalTimeZone()).thenReturn(ZoneOffset.UTC);
        // ATSPM phase (2) matches the mapped phase from SPaT (2)
        when(atspmClientService.processedEventLogs(any(), any(), eq(1)))
                .thenReturn(processedEventLog(routeConfig, rawEvent("SIG1", "2026-05-03T10:00:00", GREEN, 2)));
        when(spatService.signalGroupIndicationLogs(eq(100), any(), any()))
                .thenReturn(indicationLog(100, 1, Instant.parse("2026-05-03T10:00:00Z"), SpatSignalIndication.GREEN));

        List<AtspmSpatSignalGroupAlignmentEvent> events = service.atspmSpatSignalGroupAlignmentEvents(1, START, END);

        assertThat(events, is(empty()));
    }

    @Test
    void atspmSpatSignalGroupAlignmentEventsResultsAreSortedBySignalId() {
        RouteConfig routeConfig = routeConfig(1,
                signalConfig("SIG2", 200, true, phaseConfig(1, 2, null)),
                signalConfig("SIG1", 100, true, phaseConfig(1, 2, null)));
        when(parameters.findRouteConfig(1)).thenReturn(routeConfig);
        when(parameters.getLocalTimeZone()).thenReturn(ZoneOffset.UTC);
        // no ATSPM events at all for either signal -> both signals are misaligned
        when(atspmClientService.processedEventLogs(any(), any(), eq(1)))
                .thenReturn(processedEventLog(routeConfig));
        when(spatService.signalGroupIndicationLogs(eq(200), any(), any()))
                .thenReturn(indicationLog(200, 1, Instant.parse("2026-05-03T10:00:00Z"), SpatSignalIndication.GREEN));
        when(spatService.signalGroupIndicationLogs(eq(100), any(), any()))
                .thenReturn(indicationLog(100, 1, Instant.parse("2026-05-03T10:00:00Z"), SpatSignalIndication.GREEN));

        List<AtspmSpatSignalGroupAlignmentEvent> events = service.atspmSpatSignalGroupAlignmentEvents(1, START, END);

        assertThat(events, hasSize(2));
        assertThat(events.get(0).getSignalId(), is("SIG1"));
        assertThat(events.get(1).getSignalId(), is("SIG2"));
    }
}
