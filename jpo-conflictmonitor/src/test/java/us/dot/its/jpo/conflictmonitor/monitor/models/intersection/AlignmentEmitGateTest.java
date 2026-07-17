package us.dot.its.jpo.conflictmonitor.monitor.models.intersection;

import org.junit.Test;
import us.dot.its.jpo.conflictmonitor.monitor.models.Intersection.AlignmentEmitGate;

import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AlignmentEmitGateTest {

    @Test
    public void emitsOnFirstAndOnChangeOrSample() {
        AlignmentEmitGate gate = new AlignmentEmitGate(10_000L);

        assertTrue(gate.shouldEmit("k1", Set.of(1), Set.of(2), 0L));
        assertFalse(gate.shouldEmit("k1", Set.of(1), Set.of(2), 1_000L));
        assertTrue(gate.shouldEmit("k1", Set.of(1), Set.of(3), 1_001L));
        assertFalse(gate.shouldEmit("k1", Set.of(1), Set.of(3), 2_000L));
        assertTrue(gate.shouldEmit("k1", Set.of(1), Set.of(3), 12_000L));
    }
}
