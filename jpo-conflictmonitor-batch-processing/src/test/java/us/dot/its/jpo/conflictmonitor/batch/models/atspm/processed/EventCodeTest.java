package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import org.junit.jupiter.api.Test;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatSignalIndication;

import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Covers EventCode's raw-code parsing and SpatSignalIndication mapping.
 */
class EventCodeTest {

    @Test
    void fromCodeReturnsMatchingEventCodeForKnownCodes() {
        assertThat(EventCode.fromCode(1), is(Optional.of(EventCode.GREEN)));
        assertThat(EventCode.fromCode(8), is(Optional.of(EventCode.YELLOW)));
        assertThat(EventCode.fromCode(10), is(Optional.of(EventCode.RED)));
    }

    @Test
    void fromCodeReturnsEmptyForUnrecognizedCode() {
        assertThat(EventCode.fromCode(2), is(Optional.empty()));
        assertThat(EventCode.fromCode(-1), is(Optional.empty()));
    }

    @Test
    void fromSpatIndicationMapsEachIndicationToItsEventCode() {
        assertThat(EventCode.fromSpatIndication(SpatSignalIndication.GREEN), is(EventCode.GREEN));
        assertThat(EventCode.fromSpatIndication(SpatSignalIndication.YELLOW), is(EventCode.YELLOW));
        assertThat(EventCode.fromSpatIndication(SpatSignalIndication.RED), is(EventCode.RED));
    }
}
