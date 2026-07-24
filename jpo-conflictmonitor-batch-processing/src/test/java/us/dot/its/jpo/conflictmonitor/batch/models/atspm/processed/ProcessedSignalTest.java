package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import org.junit.jupiter.api.Test;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.Approach;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.ControllerType;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.Detector;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.DirectionType;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.LaneType;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.MovementType;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.Signal;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Covers ProcessedSignal#fromSignal, which also exercises ProcessedApproach#fromApproach
 * and ProcessedDetector#fromDetector via the approach/detector mapping chain.
 */
class ProcessedSignalTest {

    private ControllerType controllerType() {
        var ct = new ControllerType();
        ct.setControllerTypeID(5);
        ct.setDescription("NEMA TS2");
        return ct;
    }

    private MovementType movementType(int id, String description) {
        var mt = new MovementType();
        mt.setMovementTypeID(id);
        mt.setDescription(description);
        return mt;
    }

    private LaneType laneType() {
        var lt = new LaneType();
        lt.setLaneTypeID(30);
        lt.setDescription("Vehicle");
        return lt;
    }

    private Detector detector(Integer movementTypeId) {
        var d = new Detector();
        d.setLaneNumber(1);
        d.setMovementTypeID(movementTypeId);
        d.setLaneTypeID(30);
        return d;
    }

    @Test
    void fromSignalCopiesBasicScalarFields() {
        var signal = new Signal();
        signal.setSignalID("SIG1");
        signal.setPrimaryName("Main St");
        signal.setSecondaryName("1st Ave");
        signal.setLatitude("33.1");
        signal.setLongitude("-112.1");
        signal.setEnabled(true);

        ProcessedSignal ps = ProcessedSignal.fromSignal(signal, List.of(), List.of(), List.of(), List.of());

        assertThat(ps.getSignalID(), is("SIG1"));
        assertThat(ps.getPrimaryName(), is("Main St"));
        assertThat(ps.getSecondaryName(), is("1st Ave"));
        assertThat(ps.getLatitude(), is("33.1"));
        assertThat(ps.getLongitude(), is("-112.1"));
        assertThat(ps.isEnabled(), is(true));
    }

    @Test
    void fromSignalMapsControllerTypeWhenPresentInLookup() {
        var signal = new Signal();
        signal.setControllerTypeID(5);

        ProcessedSignal ps = ProcessedSignal.fromSignal(signal,
                List.of(controllerType()), List.of(), List.of(), List.of());

        assertThat(ps.getControllerType(), is("NEMA TS2"));
    }

    @Test
    void fromSignalLeavesControllerTypeNullWhenNotInLookup() {
        var signal = new Signal();
        signal.setControllerTypeID(99);

        ProcessedSignal ps = ProcessedSignal.fromSignal(signal,
                List.of(controllerType()), List.of(), List.of(), List.of());

        assertThat(ps.getControllerType(), is(nullValue()));
    }

    @Test
    void fromSignalMapsApproachesWithDetectorsIntoLanesAndMovements() {
        var approach = new Approach();
        approach.setApproachID(1);
        approach.setDirectionTypeID(10);
        approach.setProtectedPhaseNumber(2);
        approach.setPermissivePhaseNumber(6);
        approach.setPedestrianPhaseNumber(4);
        approach.setDetectors(List.of(detector(20)));

        var signal = new Signal();
        signal.setApproaches(List.of(approach));

        var directionType = new DirectionType();
        directionType.setDirectionTypeID(10);
        directionType.setDescription("Northbound");

        ProcessedSignal ps = ProcessedSignal.fromSignal(signal, List.of(),
                List.of(movementType(20, "Thru")), List.of(laneType()), List.of(directionType));

        assertThat(ps.getApproaches(), hasSize(1));
        ProcessedApproach pa = ps.getApproaches().getFirst();
        assertThat(pa.getApproachID(), is(1));
        assertThat(pa.getDirectionType(), is("Northbound"));
        assertThat(pa.getProtectedPhaseNumber(), is(2));
        assertThat(pa.getPermissivePhaseNumber(), is(6));
        assertThat(pa.getPedestrianPhaseNumber(), is(4));
        assertThat(pa.getLanes(), hasSize(1));
        Lane lane = pa.getLanes().iterator().next();
        assertThat(lane.getLaneNumber(), is(1));
        assertThat(lane.getLaneType(), is("Vehicle"));
        assertThat(lane.getMovements(), contains("Thru"));
    }

    @Test
    void fromSignalGroupsDetectorsOnTheSameLaneNumberIntoOneLaneWithMultipleMovements() {
        var approach = new Approach();
        approach.setApproachID(1);
        approach.setDetectors(List.of(
                detector(20),
                detector(21)));

        var signal = new Signal();
        signal.setApproaches(List.of(approach));

        ProcessedSignal ps = ProcessedSignal.fromSignal(signal, List.of(),
                List.of(movementType(20, "Thru"), movementType(21, "Left")),
                List.of(laneType()), List.of());

        ProcessedApproach pa = ps.getApproaches().getFirst();
        assertThat(pa.getLanes(), hasSize(1));
        Lane lane = pa.getLanes().iterator().next();
        assertThat(lane.getMovements(), containsInAnyOrder("Thru", "Left"));
    }

    @Test
    void fromSignalHandlesNullApproachesList() {
        var signal = new Signal();
        signal.setApproaches(null);

        ProcessedSignal ps = ProcessedSignal.fromSignal(signal, List.of(), List.of(), List.of(), List.of());

        assertThat(ps.getApproaches(), is(empty()));
    }
}
