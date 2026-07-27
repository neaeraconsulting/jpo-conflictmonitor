package us.dot.its.jpo.conflictmonitor.monitor.models.spat;

import org.junit.Test;
import org.testng.collections.Lists;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementEvent;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementPhaseState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedMovementState;
import us.dot.its.jpo.geojsonconverter.pojos.spat.ProcessedSpat;
import us.dot.its.jpo.geojsonconverter.pojos.spat.TimingChangeDetails;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SpatMessageCountStateTest {

    @Test
    public void contentHashIgnoresTimestampAndRevision() {
        ProcessedSpat a = spat(1000L, 5, ProcessedMovementPhaseState.STOP_AND_REMAIN, 5);
        ProcessedSpat b = spat(2000L, 5, ProcessedMovementPhaseState.STOP_AND_REMAIN, 5);
        b.setRevision(99);
        b.setOdeReceivedAt("2020-01-01T00:00:00.000Z");

        assertEquals(SpatMessageCountState.contentHash(a), SpatMessageCountState.contentHash(b));
    }

    @Test
    public void contentHashChangesWhenPhaseChanges() {
        ProcessedSpat a = spat(1000L, 5, ProcessedMovementPhaseState.STOP_AND_REMAIN, 5);
        ProcessedSpat b = spat(1000L, 5, ProcessedMovementPhaseState.PROTECTED_MOVEMENT_ALLOWED, 5);

        assertNotEquals(SpatMessageCountState.contentHash(a), SpatMessageCountState.contentHash(b));
    }

    @Test
    public void sameRevisionAndContent() {
        SpatMessageCountState a = SpatMessageCountState.fromProcessedSpat(
                spat(1000L, 3, ProcessedMovementPhaseState.STOP_AND_REMAIN, 2));
        SpatMessageCountState b = SpatMessageCountState.fromProcessedSpat(
                spat(5000L, 3, ProcessedMovementPhaseState.STOP_AND_REMAIN, 2));
        assertTrue(a.sameRevisionAndContent(b));
    }

    @Test
    public void conflictSpatFingerprintStableForSamePhases() {
        // smoke via MapSpatMessageAssessmentTopology helper is package-private static — test fingerprint logic here
        ProcessedSpat spat = spat(1000L, 1, ProcessedMovementPhaseState.STOP_AND_REMAIN, 4);
        int h1 = SpatMessageCountState.contentHash(spat);
        spat.setUtcTimeStamp(ZonedDateTime.ofInstant(Instant.ofEpochMilli(9999L), ZoneOffset.UTC));
        int h2 = SpatMessageCountState.contentHash(spat);
        assertEquals(h1, h2);
    }

    private ProcessedSpat spat(long utcMs, int revision, ProcessedMovementPhaseState phase, int signalGroup) {
        ProcessedSpat spat = new ProcessedSpat();
        spat.setUtcTimeStamp(ZonedDateTime.ofInstant(Instant.ofEpochMilli(utcMs), ZoneOffset.UTC));
        spat.setRevision(revision);
        spat.setIntersectionId(12109);
        spat.setRegion(0);
        spat.setOriginIp("127.0.0.1");

        ProcessedMovementState state = new ProcessedMovementState();
        state.setSignalGroup(signalGroup);
        ProcessedMovementEvent event = new ProcessedMovementEvent();
        event.setEventState(phase);
        TimingChangeDetails timing = new TimingChangeDetails();
        // Fixed timing so utcTimeStamp/revision changes do not affect contentHash
        timing.setMinEndTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(1_700_000_001_000L), ZoneOffset.UTC));
        event.setTiming(timing);
        state.setStateTimeSpeed(Lists.newArrayList(event));
        spat.setStates(Lists.newArrayList(state));
        return spat;
    }
}
