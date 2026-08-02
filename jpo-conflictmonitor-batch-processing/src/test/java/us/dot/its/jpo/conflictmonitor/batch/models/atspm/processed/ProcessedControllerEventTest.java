package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Covers ProcessedControllerEvent.DATE_TIME_FORMATTER: a single fractional digit is a
 * proportional fraction of MILLI_OF_SECOND's range (".6" -> 600ms, not 6ms).
 */
class ProcessedControllerEventTest {

    @Test
    void parsesATimestampWithNoFractionalSeconds() {
        LocalDateTime parsed = LocalDateTime.parse("2026-05-03T18:21:04", ProcessedControllerEvent.DATE_TIME_FORMATTER);

        assertThat(parsed, is(LocalDateTime.of(2026, 5, 3, 18, 21, 4)));
    }

    @Test
    void parsesASingleFractionalDigitAsTenthsOfASecondNotMilliseconds() {
        LocalDateTime parsed = LocalDateTime.parse("2026-05-03T18:21:04.6", ProcessedControllerEvent.DATE_TIME_FORMATTER);

        assertThat(parsed, is(LocalDateTime.of(2026, 5, 3, 18, 21, 4, 600_000_000)));
    }

    @Test
    void parsesZeroAndNineTenthsCorrectlyAtTheBoundaries() {
        LocalDateTime zeroTenths = LocalDateTime.parse("2026-05-03T18:21:04.0", ProcessedControllerEvent.DATE_TIME_FORMATTER);
        LocalDateTime nineTenths = LocalDateTime.parse("2026-05-03T18:21:04.9", ProcessedControllerEvent.DATE_TIME_FORMATTER);

        assertThat(zeroTenths.getNano(), is(0));
        assertThat(nineTenths.getNano(), is(900_000_000));
    }

    @Test
    void formatsATenthsOfASecondValueBackToASingleDigit() {
        LocalDateTime time = LocalDateTime.of(2026, 5, 3, 18, 21, 4, 600_000_000);

        String formatted = ProcessedControllerEvent.DATE_TIME_FORMATTER.format(time);

        assertThat(formatted, is("2026-05-03T18:21:04.6"));
    }

    @Test
    void formattingAlwaysIncludesTheFractionalDigitEvenWhenZero() {
        // Unlike parsing, the optional section is always printed when formatting, since
        // MILLI_OF_SECOND is always derivable from a LocalDateTime.
        LocalDateTime time = LocalDateTime.of(2026, 5, 3, 18, 21, 4);

        String formatted = ProcessedControllerEvent.DATE_TIME_FORMATTER.format(time);

        assertThat(formatted, is("2026-05-03T18:21:04.0"));
    }
}
