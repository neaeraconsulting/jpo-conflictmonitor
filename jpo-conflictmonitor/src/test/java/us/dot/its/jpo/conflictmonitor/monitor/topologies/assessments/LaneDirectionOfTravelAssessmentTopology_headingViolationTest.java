package us.dot.its.jpo.conflictmonitor.monitor.topologies.assessments;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import us.dot.its.jpo.conflictmonitor.monitor.models.assessments.LaneDirectionOfTravelAssessmentGroup;

@RunWith(Parameterized.class)
public class LaneDirectionOfTravelAssessmentTopology_headingViolationTest {

    double medianHeading;
    double expectedHeading;
    boolean expectedViolation;

    public LaneDirectionOfTravelAssessmentTopology_headingViolationTest(double medianHeading, double expectedHeading, boolean expectedViolation) {
        this.medianHeading = medianHeading;
        this.expectedHeading = expectedHeading;
        this.expectedViolation = expectedViolation;
    }

    @Parameters
    public static Collection<Object[]> getParams() {
        return Arrays.asList(new Object[][]{
            { 100.0, 110.0, false },
            { 100.0, 130.0, true },
            { 170.0, 190.0, false },
            { 170.0, 191.0, true },
            { 5.0, 355.0, false },
        });
    }

    @Test
    public void testHeadingViolation() {
        LaneDirectionOfTravelAssessmentGroup group = new LaneDirectionOfTravelAssessmentGroup();
        group.setMedianHeading(medianHeading);
        group.setExpectedHeading(expectedHeading);
        group.setTolerance(20);

        boolean violation = LaneDirectionOfTravelAssessmentTopology.headingViolation(group);

        assertThat(violation, equalTo(expectedViolation));
    }
}
