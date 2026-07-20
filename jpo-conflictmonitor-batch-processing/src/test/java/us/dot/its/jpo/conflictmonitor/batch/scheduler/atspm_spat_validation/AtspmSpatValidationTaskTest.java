package us.dot.its.jpo.conflictmonitor.batch.scheduler.atspm_spat_validation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationParameters;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.PhaseConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.RouteConfig;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.SignalConfig;
import us.dot.its.jpo.conflictmonitor.batch.events.AtspmSpatPairEvent;
import us.dot.its.jpo.conflictmonitor.batch.events.AtspmSpatSignalGroupAlignmentEvent;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed.ProcessedSignal;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.ControllerEventLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPair;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupIndicationLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SignalGroupStateLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatSignalIndication;
import us.dot.its.jpo.conflictmonitor.batch.mongo.ProcessedSpatCollectionUpdater;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmClientService;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm.AtspmTokenService;
import us.dot.its.jpo.conflictmonitor.batch.services.atspm_spat_validation.AtspmSpatValidationService;
import us.dot.its.jpo.conflictmonitor.batch.services.spat.ProcessedSpatService;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Covers AtspmSpatValidationTask#run(): the orchestration of a single scheduled route
 * comparison, including the per-signal-group 90%-threshold event trigger - checked
 * independently for each signal group, and skipped for any indication with zero
 * transitions in the window rather than treated as a failing 0%.
 */
@ExtendWith(MockitoExtension.class)
class AtspmSpatValidationTaskTest {

    private static final Instant NOW = Instant.parse("2026-05-03T11:00:00Z");

    @Mock
    private AtspmSpatValidationParameters parameters;
    @Mock
    private AtspmTokenService tokenService;
    @Mock
    private AtspmClientService clientService;
    @Mock
    private MongoTemplate mongoTemplate;
    @Mock
    private AtspmSpatValidationService atspmSpatService;
    @Mock
    private ProcessedSpatService spatService;
    @Mock
    private ProcessedSpatCollectionUpdater spatUpdater;

    private Clock clock;

    @BeforeEach
    void setUp() {
        clock = Clock.fixed(NOW, ZoneOffset.UTC);
        // Reached on every code path through run(), including the early-return for a
        // disabled route, so safe to stub unconditionally for every test.
        when(parameters.getGracePeriodOffset()).thenReturn(1);
        when(parameters.getGracePeriodOffsetUnits()).thenReturn(ChronoUnit.HOURS);
        when(parameters.getInterval()).thenReturn(1);
        when(parameters.getIntervalUnits()).thenReturn(ChronoUnit.HOURS);
        when(parameters.getLocalTimeZone()).thenReturn(ZoneOffset.UTC);
    }

    private SignalConfig signalConfig(String signalId, Integer intersectionId, boolean enabled) {
        var sc = new SignalConfig();
        sc.setSignalId(signalId);
        sc.setIntersectionId(intersectionId);
        sc.setEnabled(enabled);
        return sc;
    }

    private RouteConfig routeConfig(int routeId, SignalConfig... signals) {
        var rc = new RouteConfig();
        rc.setRouteId(routeId);
        rc.setSignals(List.of(signals));
        return rc;
    }

    private void stubFindRouteConfig(RouteConfig routeConfig) {
        when(parameters.findRouteConfig(routeConfig.getRouteId())).thenReturn(routeConfig);
    }

    private AtspmSpatValidationTask task(RouteConfig routeConfig) {
        return new AtspmSpatValidationTask(routeConfig, parameters, tokenService, clientService, clock,
                mongoTemplate, atspmSpatService, spatService, spatUpdater);
    }

    private ControllerEventLog rawEvent(String signalId, String time, int code, int phase) {
        var e = new ControllerEventLog();
        e.setSignalId(signalId);
        e.setTimestamp(time);
        e.setEventCode(code);
        e.setEventParam(phase);
        return e;
    }

    private AtspmSpatPair pair(int signalGroup, SpatSignalIndication indication, boolean paired) {
        var p = new AtspmSpatPair();
        p.setSpatSignalGroupId(signalGroup);
        p.setSpatIndication(indication);
        p.setPaired(paired);
        return p;
    }

    /** All AtspmSpatPairEvents inserted into mongoTemplate during the test so far. */
    private List<AtspmSpatPairEvent> insertedPairEvents() {
        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(mongoTemplate, atLeastOnce()).insert(captor.capture());
        return captor.getAllValues().stream()
                .filter(AtspmSpatPairEvent.class::isInstance)
                .map(AtspmSpatPairEvent.class::cast)
                .toList();
    }

    @Test
    void runFetchesTokenAndAuthenticatesBeforeQueryingAtspm() {
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", 100, false));

        task(routeConfig).run();

        InOrder inOrder = inOrder(tokenService, clientService);
        inOrder.verify(tokenService).token();
        inOrder.verify(clientService).authenticate();
    }

    @Test
    void runUpdatesProcessedSpatTimestampsBeforeQuerying() {
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", 100, false));

        task(routeConfig).run();

        verify(spatUpdater).updateTimestamp();
    }

    @Test
    void runComputesQueryWindowUsingGracePeriodAndInterval() {
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", 100, true));
        stubFindRouteConfig(routeConfig);

        task(routeConfig).run();

        ArgumentCaptor<java.time.LocalDateTime> startCaptor = ArgumentCaptor.forClass(java.time.LocalDateTime.class);
        ArgumentCaptor<java.time.LocalDateTime> endCaptor = ArgumentCaptor.forClass(java.time.LocalDateTime.class);
        verify(clientService).controllerEventLogs(startCaptor.capture(), endCaptor.capture(), eq(1));

        // gracePeriod = 1 HOUR, interval = 1 HOUR, clock fixed at NOW, zone UTC
        var expectedEnd = java.time.LocalDateTime.ofInstant(NOW.minus(1, ChronoUnit.HOURS), ZoneOffset.UTC);
        var expectedStart = expectedEnd.minusHours(1);
        assertThat(endCaptor.getValue(), is(expectedEnd));
        assertThat(startCaptor.getValue(), is(expectedStart));
    }

    @Test
    void runDoesNothingWhenRouteHasNoEnabledSignals() {
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", 100, false));

        task(routeConfig).run();

        verify(clientService, never()).controllerEventLogs(any(), any(), anyInt());
        verifyNoInteractions(mongoTemplate);
    }

    @Test
    void runSavesProcessedControllerEventLogOnlyWhenNonEmpty() {
        var phaseConfig = new PhaseConfig();
        phaseConfig.setSignalGroupId(1);
        phaseConfig.setPrimaryPhase(2);
        var signal = signalConfig("SIG1", 100, true);
        signal.setPhases(List.of(phaseConfig));
        RouteConfig routeConfig = routeConfig(1, signal);
        stubFindRouteConfig(routeConfig);

        // Case 1: raw events produce a non-empty processed log -> should be inserted
        when(clientService.controllerEventLogs(any(), any(), eq(1)))
                .thenReturn(List.of(rawEvent("SIG1", "2026-05-03T09:00:00", 1, 2)));

        task(routeConfig).run();

        verify(mongoTemplate, times(1)).insert(any(ProcessedControllerEventLog.class));

        // Case 2: no raw events -> empty processed log -> should not be inserted
        clearInvocations(mongoTemplate);
        when(clientService.controllerEventLogs(any(), any(), eq(1))).thenReturn(List.of());

        task(routeConfig).run();

        verify(mongoTemplate, never()).insert(any(ProcessedControllerEventLog.class));
    }

    @Test
    void runSavesEachSignalGroupAlignmentEventReturnedByTheService() {
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", 100, true));
        stubFindRouteConfig(routeConfig);
        var event1 = new AtspmSpatSignalGroupAlignmentEvent();
        event1.setSignalId("SIG1");
        var event2 = new AtspmSpatSignalGroupAlignmentEvent();
        event2.setSignalId("SIG2");
        when(atspmSpatService.atspmSpatSignalGroupAlignmentEvents(eq(1), any(), any()))
                .thenReturn(List.of(event1, event2));

        task(routeConfig).run();

        verify(mongoTemplate).insert(event1);
        verify(mongoTemplate).insert(event2);
    }

    @Test
    void runSavesEachAtspmSpatPairLogReturnedByTheService() {
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", 100, true));
        stubFindRouteConfig(routeConfig);
        var pairLog = new AtspmSpatPairLog();
        pairLog.setAtspmSpatPairs(new ArrayList<>());
        when(atspmSpatService.atpsmSpatLogs(eq(1), any(), any())).thenReturn(List.of(pairLog));

        task(routeConfig).run();

        verify(mongoTemplate).insert(pairLog);
    }

    @Test
    void runWritesAPairEventForEachSignalGroupBelowTheNinetyPercentThreshold() {
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", 100, true));
        stubFindRouteConfig(routeConfig);

        var pairLog = new AtspmSpatPairLog();
        pairLog.setAtspmSpatPairs(new ArrayList<>(List.of(
                pair(1, SpatSignalIndication.GREEN, true),
                pair(1, SpatSignalIndication.GREEN, false), // group 1: 50% green paired -> below threshold
                pair(2, SpatSignalIndication.GREEN, true)))); // group 2: 100% green paired -> above threshold
        when(atspmSpatService.atpsmSpatLogs(eq(1), any(), any())).thenReturn(List.of(pairLog));

        task(routeConfig).run();

        List<AtspmSpatPairEvent> events = insertedPairEvents();
        assertThat(events, hasSize(1));
        assertThat(events.get(0).getSignalGroup(), is(1));
    }

    @Test
    void runDoesNotWritePairEventWhenAllSignalGroupsAreAboveThreshold() {
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", 100, true));
        stubFindRouteConfig(routeConfig);

        var pairLog = new AtspmSpatPairLog();
        pairLog.setAtspmSpatPairs(new ArrayList<>(List.of(
                pair(1, SpatSignalIndication.GREEN, true),
                pair(1, SpatSignalIndication.RED, true))));
        when(atspmSpatService.atpsmSpatLogs(eq(1), any(), any())).thenReturn(List.of(pairLog));

        task(routeConfig).run();

        verify(mongoTemplate, never()).insert(any(AtspmSpatPairEvent.class));
    }

    @Test
    void runDoesNotTreatAZeroCountIndicationAsAFailingPercentage() {
        // Signal group 1 has only GREEN pairs (100% paired) - no RED or YELLOW pairs at all
        // in this window. A color with zero transitions must not be treated as a failing 0%.
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", 100, true));
        stubFindRouteConfig(routeConfig);

        var pairLog = new AtspmSpatPairLog();
        pairLog.setAtspmSpatPairs(new ArrayList<>(List.of(
                pair(1, SpatSignalIndication.GREEN, true))));
        when(atspmSpatService.atpsmSpatLogs(eq(1), any(), any())).thenReturn(List.of(pairLog));

        task(routeConfig).run();

        verify(mongoTemplate, never()).insert(any(AtspmSpatPairEvent.class));
    }

    @Test
    void runSavesSpatStateAndIndicationLogsForEachConfiguredSignalWithAnIntersectionId() {
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", 100, true));
        stubFindRouteConfig(routeConfig);
        var stateLog = new SignalGroupStateLog();
        var indicationLog = new SignalGroupIndicationLog();
        when(spatService.signalGroupLogs(eq(100), any(), any())).thenReturn(stateLog);
        when(spatService.signalGroupIndicationLogs(eq(100), any(), any())).thenReturn(indicationLog);

        task(routeConfig).run();

        verify(mongoTemplate).insert(stateLog);
        verify(mongoTemplate).insert(indicationLog);
    }

    @Test
    void runSkipsSpatLogsForSignalsMissingOrWithInvalidIntersectionId() {
        RouteConfig routeConfig = routeConfig(1,
                signalConfig("SIG1", null, true),
                signalConfig("SIG2", 0, true));
        stubFindRouteConfig(routeConfig);

        task(routeConfig).run();

        verify(spatService, never()).signalGroupLogs(anyInt(), any(), any());
        verify(spatService, never()).signalGroupIndicationLogs(anyInt(), any(), any());
    }

    @Test
    void runSavesProcessedSignalConfigForEachSignalWhenPresent() {
        RouteConfig routeConfig = routeConfig(1, signalConfig("SIG1", 100, true));
        stubFindRouteConfig(routeConfig);
        var processedSignal = new ProcessedSignal();
        when(clientService.processedSignalConfig("SIG1")).thenReturn(processedSignal);

        task(routeConfig).run();

        verify(mongoTemplate).insert(processedSignal);
    }
}
