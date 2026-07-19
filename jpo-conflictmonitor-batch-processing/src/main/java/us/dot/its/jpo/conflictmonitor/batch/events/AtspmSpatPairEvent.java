package us.dot.its.jpo.conflictmonitor.batch.events;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPair;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatStatistics;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.SignalGroupStatistics;

import java.time.Instant;
import java.util.List;

/**
 * Events are produced per intersection, per signal group, and per time period if the
 * percentage of paired ATSPM and SPAT events for that signal group is less than 90% for
 * any indication (RED, YELLOW, or GREEN).
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("CmAtspmSpatPairEvent")
public class AtspmSpatPairEvent extends Event {

    public AtspmSpatPairEvent() {
        super("AtspmSpatPair");
    }

    /**
     * ATSPM Route ID
     */
    private int routeId;

    /**
     * ATSPM Signal ID
     */
    private String signalId;

    /**
     * SPAT Signal Group ID that this event's percentages are scoped to
     */
    private int signalGroup;

    /**
     * Start time of batch period
     */
    private Instant startTime;

    /**
     * End time of batch period
     */
    private Instant endTime;

    /**
     * ATSMP-SPAT event pairs for this signal group
     */
    private List<AtspmSpatPair> atspmSpatPairs;

    private double percentPaired;
    private double percentRedPaired;
    private double percentYellowPaired;
    private double percentGreenPaired;

    /**
     * Per-signal-group breakdown for the whole signal, included for context alongside the
     * single signal group (above) that triggered this event.
     */
    private AtspmSpatStatistics signalGroupStatistics;

    public static AtspmSpatPairEvent fromLog(AtspmSpatPairLog log, SignalGroupStatistics signalGroupStats) {
        AtspmSpatPairEvent event = new AtspmSpatPairEvent();
        event.setIntersectionID(log.getIntersectionId());
        event.setRouteId(log.getRouteId());
        event.setSignalId(log.getSignalId());
        event.setSignalGroup(signalGroupStats.signalGroup());
        event.setStartTime(log.getStartTime());
        event.setEndTime(log.getEndTime());
        event.setPercentPaired(signalGroupStats.percentAllPaired());
        event.setPercentRedPaired(signalGroupStats.percentRedPaired());
        event.setPercentYellowPaired(signalGroupStats.percentYellowPaired());
        event.setPercentGreenPaired(signalGroupStats.percentGreenPaired());
        event.setSignalGroupStatistics(log.getSignalGroupStatistics());
        List<AtspmSpatPair> groupPairs = log.getAtspmSpatPairs().stream()
                .filter(pair -> pair.getSpatSignalGroupId() != null
                        && pair.getSpatSignalGroupId() == signalGroupStats.signalGroup())
                .toList();
        event.setAtspmSpatPairs(groupPairs);
        return event;
    }

}
