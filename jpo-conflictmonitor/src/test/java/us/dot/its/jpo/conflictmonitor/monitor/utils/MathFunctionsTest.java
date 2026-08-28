package us.dot.its.jpo.conflictmonitor.monitor.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

public class MathFunctionsTest {

    @Test
    public void testGetMedianHeadingEmptyList() {
        assertThat(MathFunctions.getMedianHeading(new ArrayList<>()), closeTo(0.0, 0.0001));
    }

    @Test
    public void testGetMedianHeadingNoWraparound() {
        ArrayList<Double> headings = new ArrayList<>(Arrays.asList(100.0, 105.0, 110.0, 120.0));
        double expected = MathFunctions.getMedian(new ArrayList<>(headings));
        assertThat(MathFunctions.getMedianHeading(headings), closeTo(expected, 0.0001));
    }

    @Test
    public void testGetMedianHeadingOddCountWraparound() {
        ArrayList<Double> headings = new ArrayList<>(Arrays.asList(359.0, 0.0, 1.0));
        assertThat(MathFunctions.getMedianHeading(headings), closeTo(0.0, 0.0001));
    }

    @Test
    public void testGetMedianHeadingEvenCountWraparoundRegression() {
        ArrayList<Double> headings = new ArrayList<>(Arrays.asList(358.0, 359.0, 1.0, 2.0));

        double naiveMedian = MathFunctions.getMedian(new ArrayList<>(headings));
        assertThat(naiveMedian, closeTo(180.0, 0.0001));

        double circularMedian = MathFunctions.getMedianHeading(headings);
        assertThat(circularMedian, closeTo(0.0, 0.0001));
    }

    @Test
    public void testGetMedianHeadingRealisticScatterAroundZero() {
        ArrayList<Double> headings = new ArrayList<>(Arrays.asList(355.0, 358.0, 2.0, 5.0, 359.0, 1.0));

        double naiveMedian = MathFunctions.getMedian(new ArrayList<>(headings));
        assertThat(naiveMedian, closeTo(180.0, 0.0001));

        double circularMedian = MathFunctions.getMedianHeading(headings);
        assertThat(circularMedian, closeTo(0.0, 0.0001));
    }

    @Test
    public void testGetMedianHeadingWraparoundNearOppositeBoundary() {
        ArrayList<Double> headings = new ArrayList<>(Arrays.asList(170.0, 175.0, 185.0, 190.0));
        assertThat(MathFunctions.getMedianHeading(headings), closeTo(180.0, 0.0001));
    }
}
