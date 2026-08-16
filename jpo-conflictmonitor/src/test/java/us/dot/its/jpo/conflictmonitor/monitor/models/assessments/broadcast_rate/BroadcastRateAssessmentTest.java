package us.dot.its.jpo.conflictmonitor.monitor.models.assessments.broadcast_rate;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class BroadcastRateAssessmentTest {

    private static SpatBroadcastRateAssessment assessment(
            int numberOfMessages,
            int numberOfPairViolations,
            int numberOfDurationViolations,
            double percentToPass,
            int maxPairSeparationMs,
            int maxAllowedPairSeparationMs) {
        var assessment = new SpatBroadcastRateAssessment();
        assessment.setNumberOfMessages(numberOfMessages);
        assessment.setNumberOfPairViolations(numberOfPairViolations);
        assessment.setNumberOfDurationViolations(numberOfDurationViolations);
        assessment.setPercentToPass(percentToPass);
        assessment.setMaxPairSeparationMs(maxPairSeparationMs);
        assessment.setMaxAllowedPairSeparationMs(maxAllowedPairSeparationMs);
        return assessment;
    }

    @Test
    public void testIsPairComparisonPass_underThreshold() {
        var assessment = assessment(100, 5, 0, 90.0, 0, 0);
        assertThat(assessment.isPairComparisonPass(), is(true));
    }

    @Test
    public void testIsPairComparisonPass_atThreshold() {
        var assessment = assessment(100, 10, 0, 90.0, 0, 0);
        assertThat(assessment.isPairComparisonPass(), is(true));
    }

    @Test
    public void testIsPairComparisonPass_overThreshold() {
        var assessment = assessment(100, 11, 0, 90.0, 0, 0);
        assertThat(assessment.isPairComparisonPass(), is(false));
    }

    @Test
    public void testIsPairComparisonPass_noMessages() {
        var assessment = assessment(0, 0, 0, 90.0, 0, 0);
        assertThat(assessment.isPairComparisonPass(), is(true));
    }

    @Test
    public void testIsDurationComparisonPass_underThreshold() {
        var assessment = assessment(100, 0, 5, 90.0, 0, 0);
        assertThat(assessment.isDurationComparisonPass(), is(true));
    }

    @Test
    public void testIsDurationComparisonPass_atThreshold() {
        var assessment = assessment(100, 0, 10, 90.0, 0, 0);
        assertThat(assessment.isDurationComparisonPass(), is(true));
    }

    @Test
    public void testIsDurationComparisonPass_overThreshold() {
        var assessment = assessment(100, 0, 11, 90.0, 0, 0);
        assertThat(assessment.isDurationComparisonPass(), is(false));
    }

    @Test
    public void testIsDurationComparisonPass_noMessages() {
        var assessment = assessment(0, 0, 0, 90.0, 0, 0);
        assertThat(assessment.isDurationComparisonPass(), is(true));
    }

    @Test
    public void testIsMaxPairSeparationPass_under() {
        var assessment = assessment(100, 0, 0, 90.0, 500, 1000);
        assertThat(assessment.isMaxPairSeparationPass(), is(true));
    }

    @Test
    public void testIsMaxPairSeparationPass_atThreshold() {
        var assessment = assessment(100, 0, 0, 90.0, 1000, 1000);
        assertThat(assessment.isMaxPairSeparationPass(), is(true));
    }

    @Test
    public void testIsMaxPairSeparationPass_over() {
        var assessment = assessment(100, 0, 0, 90.0, 1001, 1000);
        assertThat(assessment.isMaxPairSeparationPass(), is(false));
    }

    @Test
    public void testIsPassWithMaxPairSeparation_allPass() {
        var assessment = assessment(100, 5, 5, 90.0, 500, 1000);
        assertThat(assessment.isPassWithMaxPairSeparation(), is(true));
    }

    @Test
    public void testIsPassWithMaxPairSeparation_pairFailsOnly() {
        var assessment = assessment(100, 11, 0, 90.0, 500, 1000);
        assertThat(assessment.isPassWithMaxPairSeparation(), is(false));
    }

    @Test
    public void testIsPassWithMaxPairSeparation_durationFailsOnly() {
        var assessment = assessment(100, 0, 11, 90.0, 500, 1000);
        assertThat(assessment.isPassWithMaxPairSeparation(), is(false));
    }

    @Test
    public void testIsPassWithMaxPairSeparation_maxPairSeparationFailsOnly() {
        var assessment = assessment(100, 0, 0, 90.0, 1001, 1000);
        assertThat(assessment.isPassWithMaxPairSeparation(), is(false));
    }

    @Test
    public void testIsPass_bothPass() {
        var assessment = assessment(100, 5, 5, 90.0, 500, 1000);
        assertThat(assessment.isPass(), is(true));
    }

    @Test
    public void testIsPass_pairFails() {
        var assessment = assessment(100, 11, 0, 90.0, 500, 1000);
        assertThat(assessment.isPass(), is(false));
    }

    @Test
    public void testIsPass_durationFails() {
        var assessment = assessment(100, 0, 11, 90.0, 500, 1000);
        assertThat(assessment.isPass(), is(false));
    }

    @Test
    public void testIsPass_ignoresMaxPairSeparation() {
        var assessment = assessment(100, 0, 0, 90.0, 1001, 1000);
        assertThat(assessment.isPass(), is(true));
        assertThat(assessment.isPassWithMaxPairSeparation(), is(false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetPercentToPass_belowZero() {
        assessment(100, 0, 0, -0.1, 0, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetPercentToPass_aboveHundred() {
        assessment(100, 0, 0, 100.1, 0, 0);
    }

    @Test
    public void testSetPercentToPass_boundaryZero() {
        var assessment = assessment(100, 0, 0, 0.0, 0, 0);
        assertThat(assessment.getPercentToPass(), equalTo(0.0));
    }

    @Test
    public void testSetPercentToPass_boundaryHundred() {
        var assessment = assessment(100, 0, 0, 100.0, 0, 0);
        assertThat(assessment.getPercentToPass(), equalTo(100.0));
    }
}
