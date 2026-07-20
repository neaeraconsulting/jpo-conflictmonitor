package us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat;

import org.junit.jupiter.api.Test;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatSignalIndication;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Covers AtspmSpatPairLog's derived percentage/statistics getters, which drive the
 * AtspmSpatPairEvent 90%-threshold trigger in AtspmSpatValidationTask.
 */
class AtspmSpatPairLogTest {

    private AtspmSpatPair pair(int signalGroup, SpatSignalIndication indication, boolean paired) {
        var p = new AtspmSpatPair();
        p.setSpatSignalGroupId(signalGroup);
        p.setSpatIndication(indication);
        p.setPaired(paired);
        return p;
    }

    @Test
    void getPercentPairedReturnsZeroWhenPairListIsNullOrEmpty() {
        var nullList = new AtspmSpatPairLog();
        assertThat(nullList.getPercentPaired(), is(0.0));

        var emptyList = new AtspmSpatPairLog();
        emptyList.setAtspmSpatPairs(new ArrayList<>());
        assertThat(emptyList.getPercentPaired(), is(0.0));
    }

    @Test
    void getPercentPairedComputesOverallPairedPercentageAcrossAllIndications() {
        var log = new AtspmSpatPairLog();
        log.setAtspmSpatPairs(List.of(
                pair(1, SpatSignalIndication.GREEN, true),
                pair(1, SpatSignalIndication.RED, true),
                pair(1, SpatSignalIndication.YELLOW, false),
                pair(2, SpatSignalIndication.GREEN, false)));

        assertThat(log.getPercentPaired(), is(closeTo(50.0, 0.001)));
    }

    @Test
    void getPercentGreenRedYellowPairedFilterByIndicationColorOnly() {
        var log = new AtspmSpatPairLog();
        log.setAtspmSpatPairs(List.of(
                pair(1, SpatSignalIndication.GREEN, true),
                pair(1, SpatSignalIndication.GREEN, false),
                pair(1, SpatSignalIndication.RED, true),
                pair(1, SpatSignalIndication.YELLOW, true),
                pair(1, SpatSignalIndication.YELLOW, true)));

        assertThat(log.getPercentGreenPaired(), is(closeTo(50.0, 0.001)));
        assertThat(log.getPercentRedPaired(), is(closeTo(100.0, 0.001)));
        assertThat(log.getPercentYellowPaired(), is(closeTo(100.0, 0.001)));
    }

    @Test
    void getSignalGroupStatisticsBreaksDownPercentagesPerSignalGroup() {
        var log = new AtspmSpatPairLog();
        log.setAtspmSpatPairs(List.of(
                pair(1, SpatSignalIndication.GREEN, true),
                pair(1, SpatSignalIndication.GREEN, false),
                pair(2, SpatSignalIndication.GREEN, true)));

        AtspmSpatStatistics stats = log.getSignalGroupStatistics();

        assertThat(stats.keySet(), containsInAnyOrder(1, 2));
        assertThat(stats.get(1).percentGreenPaired(), is(closeTo(50.0, 0.001)));
        assertThat(stats.get(2).percentGreenPaired(), is(closeTo(100.0, 0.001)));
    }

    @Test
    void getSignalGroupStatisticsHandlesSignalGroupWithNoPairsOfAGivenColor() {
        var log = new AtspmSpatPairLog();
        log.setAtspmSpatPairs(List.of(
                pair(1, SpatSignalIndication.GREEN, true)));

        AtspmSpatStatistics stats = log.getSignalGroupStatistics();

        assertThat(stats.get(1).percentYellowPaired(), is(0.0));
        assertThat(stats.get(1).percentRedPaired(), is(0.0));
        assertThat(stats.get(1).percentGreenPaired(), is(closeTo(100.0, 0.001)));
    }
}
