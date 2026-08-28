package us.dot.its.jpo.conflictmonitor.monitor.analytics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.junit.Test;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import us.dot.its.jpo.conflictmonitor.monitor.algorithms.lane_direction_of_travel.LaneDirectionOfTravelParameters;
import us.dot.its.jpo.conflictmonitor.monitor.models.Intersection.Intersection;
import us.dot.its.jpo.conflictmonitor.monitor.models.Intersection.Lane;
import us.dot.its.jpo.conflictmonitor.monitor.models.Intersection.LaneSegment;
import us.dot.its.jpo.conflictmonitor.monitor.models.Intersection.VehiclePath;
import us.dot.its.jpo.conflictmonitor.monitor.models.bsm.BsmAggregator;
import us.dot.its.jpo.conflictmonitor.monitor.models.events.LaneDirectionOfTravelEvent;
import us.dot.its.jpo.conflictmonitor.testutils.BsmTestUtils;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;

public class LaneDirectionOfTravelAnalyticsTest {

    @Test
    public void testGetLaneDirectionEvents_BsmMissingHeadingDoesNotThrow() {
        double refLon = -105.0;
        double refLat = 40.0;

        var bsmWithHeading = BsmTestUtils.processedBsmWithPosition(Instant.ofEpochMilli(1000L), "A", refLon, refLat, 1500.0);
        bsmWithHeading.getProperties().setHeading(5.0);

        var bsmWithoutHeading = BsmTestUtils.processedBsmWithPosition(Instant.ofEpochMilli(2000L), "A", refLon, refLat, 1500.0);
        bsmWithoutHeading.getProperties().setHeading(null);

        var bsmAggregator = new BsmAggregator();
        bsmAggregator.add(bsmWithHeading);
        bsmAggregator.add(bsmWithoutHeading);

        var intersection = new Intersection();
        intersection.setReferencePoint(new CoordinateXY(refLon, refLat));

        var path = new VehiclePath(bsmAggregator, intersection, 15.0, 20.0, new ProcessedSpat());

        var lane = new Lane();
        lane.setId(12);
        lane.setRegion(0);

        var factory = new GeometryFactory();
        Point segmentStart = factory.createPoint(new CoordinateXY(0, 0));
        Point segmentEnd = factory.createPoint(new CoordinateXY(0, 1000));
        LaneSegment segment = new LaneSegment(segmentStart, segmentEnd, 366, true, 1, factory);

        HashMap<LaneSegment, ArrayList<Integer>> segmentBsmMap = new HashMap<>();
        segmentBsmMap.put(segment, new ArrayList<>(Arrays.asList(0, 1)));

        LaneDirectionOfTravelAnalytics analytics = new LaneDirectionOfTravelAnalytics();
        LaneDirectionOfTravelParameters parameters = new LaneDirectionOfTravelParameters();
        parameters.setMinimumPointsPerSegment(0);
        analytics.parameters = parameters;

        ArrayList<LaneDirectionOfTravelEvent> events = analytics.getLaneDirectionEvents(path, lane, segmentBsmMap);

        assertThat(events.size(), equalTo(1));
        LaneDirectionOfTravelEvent event = events.getFirst();
        assertThat(event.getAggregateBSMCount(), equalTo(2));
        assertThat(event.getMedianVehicleHeading(), closeTo(5.0, 0.0001));
    }
}
