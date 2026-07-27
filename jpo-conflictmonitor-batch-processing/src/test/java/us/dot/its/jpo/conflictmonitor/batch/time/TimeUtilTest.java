package us.dot.its.jpo.conflictmonitor.batch.time;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;

/**
 * Covers TimeUtil#localTimeToInstantAtZone, including the two-step DST-offset
 * approximation described in its javadoc.
 */
class TimeUtilTest {

    @Test
    void convertsLocalTimeCorrectlyForNonDstZone() {
        // America/Phoenix does not observe DST - always UTC-7
        ZoneId phoenix = ZoneId.of("America/Phoenix");
        LocalDateTime local = LocalDateTime.of(2026, 5, 3, 10, 0, 0);
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

        Instant result = TimeUtil.localTimeToInstantAtZone(local, phoenix, clock);

        assertThat(result, is(local.toInstant(ZoneOffset.of("-07:00"))));
    }

    @Test
    void convertsLocalTimeCorrectlyForDstZoneOutsideTransitionWindow() {
        // Mid-summer America/Denver is solidly in Mountain Daylight Time (UTC-6)
        ZoneId denver = ZoneId.of("America/Denver");
        LocalDateTime local = LocalDateTime.of(2026, 7, 1, 10, 0, 0);
        Clock clock = Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC);

        Instant result = TimeUtil.localTimeToInstantAtZone(local, denver, clock);

        assertThat(result, is(local.toInstant(ZoneOffset.of("-06:00"))));
    }

    @Test
    void approximatesReasonablyNearSpringForwardTransition() {
        // 2026-03-08 02:00-03:00 local America/Denver time does not exist (spring forward gap).
        // Per the javadoc, the result may be off by an hour near a transition; assert it lands
        // on one of the two plausible offsets rather than throwing or producing something else.
        ZoneId denver = ZoneId.of("America/Denver");
        LocalDateTime local = LocalDateTime.of(2026, 3, 8, 2, 30, 0);
        Clock clock = Clock.fixed(local.toInstant(ZoneOffset.of("-07:00")), ZoneOffset.UTC);

        Instant result = TimeUtil.localTimeToInstantAtZone(local, denver, clock);

        assertThat(result, anyOf(
                is(local.toInstant(ZoneOffset.of("-07:00"))),
                is(local.toInstant(ZoneOffset.of("-06:00")))));
    }

    @Test
    void approximatesReasonablyNearFallBackTransition() {
        // 2026-11-01 01:00-02:00 local America/Denver time is ambiguous (fall back repeats it).
        // Either offset is a legitimate interpretation.
        ZoneId denver = ZoneId.of("America/Denver");
        LocalDateTime local = LocalDateTime.of(2026, 11, 1, 1, 30, 0);
        Clock clock = Clock.fixed(local.toInstant(ZoneOffset.of("-06:00")), ZoneOffset.UTC);

        Instant result = TimeUtil.localTimeToInstantAtZone(local, denver, clock);

        assertThat(result, anyOf(
                is(local.toInstant(ZoneOffset.of("-06:00"))),
                is(local.toInstant(ZoneOffset.of("-07:00")))));
    }
}
