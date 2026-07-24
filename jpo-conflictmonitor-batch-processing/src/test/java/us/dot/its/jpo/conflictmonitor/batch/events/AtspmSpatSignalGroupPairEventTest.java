package us.dot.its.jpo.conflictmonitor.batch.events;

import org.junit.jupiter.api.Test;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPair;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.SignalGroupStatistics;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatSignalIndication;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Covers AtspmSpatSignalGroupPairEvent#fromLog(AtspmSpatPairLog, SignalGroupStatistics), the
 * factory that scopes an event's pairs and percentages to a single signal group.
 */
class AtspmSpatSignalGroupPairEventTest {

    private static final Instant START = Instant.parse("2026-05-03T10:00:00Z");
    private static final Instant END = Instant.parse("2026-05-03T11:00:00Z");

    private AtspmSpatPair pair(int signalGroup, SpatSignalIndication indication, boolean paired) {
        var p = new AtspmSpatPair();
        p.setSpatSignalGroupId(signalGroup);
        p.setSpatIndication(indication);
        p.setPaired(paired);
        return p;
    }

    private AtspmSpatPairLog log(List<AtspmSpatPair> pairs) {
        var log = new AtspmSpatPairLog();
        log.setRouteId(1);
        log.setSignalId("SIG1");
        log.setIntersectionId(100);
        log.setStartTime(START);
        log.setEndTime(END);
        log.setAtspmSpatPairs(pairs);
        return log;
    }

    @Test
    void fromLogCopiesRouteSignalAndTimeFieldsFromTheLog() {
        AtspmSpatPairLog log = log(new ArrayList<>(List.of(pair(1, SpatSignalIndication.GREEN, true))));
        SignalGroupStatistics stats = log.getSignalGroupStatistics().get(1);

        AtspmSpatSignalGroupPairEvent event = AtspmSpatSignalGroupPairEvent.fromLog(log, stats);

        assertThat(event.getRouteId(), is(1));
        assertThat(event.getSignalId(), is("SIG1"));
        assertThat(event.getIntersectionID(), is(100));
        assertThat(event.getStartTime(), is(START));
        assertThat(event.getEndTime(), is(END));
    }

    @Test
    void fromLogSetsSignalGroupAndPercentagesFromTheGivenStatistics() {
        AtspmSpatPairLog log = log(new ArrayList<>(List.of(
                pair(1, SpatSignalIndication.GREEN, true),
                pair(1, SpatSignalIndication.GREEN, false),
                pair(2, SpatSignalIndication.GREEN, true))));
        SignalGroupStatistics stats1 = log.getSignalGroupStatistics().get(1);

        AtspmSpatSignalGroupPairEvent event = AtspmSpatSignalGroupPairEvent.fromLog(log, stats1);

        assertThat(event.getSignalGroup(), is(1));
        assertThat(event.getPercentGreenPaired(), is(closeTo(50.0, 0.001)));
        assertThat(event.getPercentPaired(), is(closeTo(stats1.percentAllPaired(), 0.001)));
    }

    @Test
    void fromLogFiltersAtspmSpatPairsToJustTheGivenSignalGroup() {
        AtspmSpatPairLog log = log(new ArrayList<>(List.of(
                pair(1, SpatSignalIndication.GREEN, true),
                pair(2, SpatSignalIndication.RED, true))));
        SignalGroupStatistics stats1 = log.getSignalGroupStatistics().get(1);

        AtspmSpatSignalGroupPairEvent event = AtspmSpatSignalGroupPairEvent.fromLog(log, stats1);

        assertThat(event.getAtspmSpatPairs(), hasSize(1));
        assertThat(event.getAtspmSpatPairs().getFirst().getSpatSignalGroupId(), is(1));
    }

    @Test
    void fromLogRetainsTheFullSignalGroupStatisticsMapAsContext() {
        AtspmSpatPairLog log = log(new ArrayList<>(List.of(
                pair(1, SpatSignalIndication.GREEN, true),
                pair(2, SpatSignalIndication.RED, true))));
        SignalGroupStatistics stats1 = log.getSignalGroupStatistics().get(1);

        AtspmSpatSignalGroupPairEvent event = AtspmSpatSignalGroupPairEvent.fromLog(log, stats1);

        assertThat(event.getSignalGroupStatistics().keySet(), containsInAnyOrder(1, 2));
    }
}
