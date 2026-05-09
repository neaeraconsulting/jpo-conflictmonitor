package us.dot.its.jpo.conflictmonitor.batch.models.atspm.processed;

import lombok.Data;

import us.dot.its.jpo.conflictmonitor.batch.models.atspm.raw.ControllerEventLog;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Optional;

@Data
public class ProcessedControllerEvent {
    private String signalId;
    private Instant timestamp;
    private EventCode eventCode;
    private int phase;
    private int secondaryPhase;

    // Formatter to parse timestamps with or without a tenth of second
    // eg. "2026-05-03T18:21:04.6"
    // or "2026-05-03T18:21:04"
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
                    .optionalStart()
                    .appendLiteral('.')
                    .appendFraction(ChronoField.MILLI_OF_SECOND, 1, 1, false)
                    .optionalEnd()
                    .toFormatter();
    /**
     * Process the event log item.
     * @param event The raw event log item
     * @return The processed event, or empty if the event code is not interesting to this algorithm
     */
    public static Optional<ProcessedControllerEvent> fromControllerEventLog(ControllerEventLog event, Clock clock, ZoneId localTimeZone) {
        if (event == null) return Optional.empty();
        int code = event.getEventCode();
        Optional<EventCode> eventCodeOpt =  EventCode.fromCode(code);
        if (eventCodeOpt.isEmpty()) return Optional.empty();
        var pce = new ProcessedControllerEvent();
        pce.eventCode = eventCodeOpt.get();
        pce.signalId = event.getSignalId();


        // Get zone offset from time zone id
        Instant now = clock.instant();
        // 0-order approximation to offset from current time
        ZoneOffset zoneOffset0 = localTimeZone.getRules().getOffset(now);
        Instant timestamp0 = LocalDateTime.parse(event.getTimestamp(), DATE_TIME_FORMATTER).toInstant(zoneOffset0);
        // Better guess from the given timestamp
        ZoneOffset zoneOffset1 = localTimeZone.getRules().getOffset(timestamp0);
        pce.timestamp = LocalDateTime.parse(event.getTimestamp(), DATE_TIME_FORMATTER).toInstant(zoneOffset1);
        // Note this timestamp might not be correct within 1 hour of DST 'fall back' or 'spring forward', but will
        // always work for the 'America/Phoenix' time zone which doesn't use DST.
        // TODO fix edge case, live with it for now for MCDOT

        pce.phase = event.getEventParam();
        return Optional.of(pce);
    }
}
