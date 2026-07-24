package us.dot.its.jpo.conflictmonitor.batch.events;

import org.junit.jupiter.api.Test;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPair;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatSignalIndication;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Covers AtspmSpatPairEvent#fromLog(AtspmSpatPairLog), the factory that builds the blended,
 * intersection-level event from a whole AtspmSpatPairLog (all signal groups combined), kept
 * for compatibility with existing consumers of the CmAtspmSpatPairEvent collection.
 */
class AtspmSpatPairEventTest {

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

        AtspmSpatPairEvent event = AtspmSpatPairEvent.fromLog(log);

        assertThat(event.getRouteId(), is(1));
        assertThat(event.getSignalId(), is("SIG1"));
        assertThat(event.getIntersectionID(), is(100));
        assertThat(event.getStartTime(), is(START));
        assertThat(event.getEndTime(), is(END));
    }

    @Test
    void fromLogBlendsPercentagesAcrossAllSignalGroups() {
        AtspmSpatPairLog log = log(new ArrayList<>(List.of(
                pair(1, SpatSignalIndication.GREEN, true),
                pair(1, SpatSignalIndication.GREEN, false), // group 1: 1/2 green paired
                pair(2, SpatSignalIndication.GREEN, true)))); // group 2: 2/2 green paired

        AtspmSpatPairEvent event = AtspmSpatPairEvent.fromLog(log);

        // blended: 2 of 3 green pairs paired = 66.67%, matching log.getPercentGreenPaired()
        assertThat(event.getPercentGreenPaired(), is(closeTo(log.getPercentGreenPaired(), 0.001)));
        assertThat(event.getPercentGreenPaired(), is(closeTo(66.667, 0.01)));
    }

    @Test
    void fromLogIncludesAllSignalGroupsPairsUnfiltered() {
        AtspmSpatPairLog log = log(new ArrayList<>(List.of(
                pair(1, SpatSignalIndication.GREEN, true),
                pair(2, SpatSignalIndication.RED, true))));

        AtspmSpatPairEvent event = AtspmSpatPairEvent.fromLog(log);

        assertThat(event.getAtspmSpatPairs(), hasSize(2));
    }

    @Test
    void fromLogIncludesPerSignalGroupStatisticsAsContext() {
        AtspmSpatPairLog log = log(new ArrayList<>(List.of(
                pair(1, SpatSignalIndication.GREEN, true),
                pair(2, SpatSignalIndication.RED, true))));

        AtspmSpatPairEvent event = AtspmSpatPairEvent.fromLog(log);

        assertThat(event.getSignalGroupStatistics().keySet(), containsInAnyOrder(1, 2));
    }
}
