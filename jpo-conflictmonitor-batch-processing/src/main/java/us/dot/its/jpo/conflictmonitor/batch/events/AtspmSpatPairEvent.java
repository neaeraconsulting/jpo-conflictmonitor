package us.dot.its.jpo.conflictmonitor.batch.events;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPair;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatStatistics;

import java.time.Instant;
import java.util.List;

/**
 * Events are produced per intersection and per time period if the percentage
 * of paired ATSPM and SPAT events is less than 90% for any indication (RED, YELLOW, or GREEN)
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
     * Start time of batch period
     */
    private Instant startTime;

    /**
     * End time of batch period
     */
    private Instant endTime;

    /**
     * ATSMP-SPAT event pairs
     */
    private List<AtspmSpatPair> atspmSpatPairs;

    /**
     * Error message, or null of no errors
     */
    private String error;

    private double percentPaired;
    private double percentRedPaired;
    private double percentYellowPaired;
    private double percentGreenPaired;
    private AtspmSpatStatistics signalGroupStatistics;

    public static AtspmSpatPairEvent fromLog(AtspmSpatPairLog log) {
        AtspmSpatPairEvent event = new AtspmSpatPairEvent();
        event.setIntersectionID(log.getIntersectionId());
        event.setAtspmSpatPairs(log.getAtspmSpatPairs());
        event.setError(log.getError());
        event.setRouteId(log.getRouteId());
        event.setSignalId(log.getSignalId());
        event.setStartTime(log.getStartTime());
        event.setEndTime(log.getEndTime());
        event.setPercentPaired(log.getPercentPaired());
        event.setPercentRedPaired(log.getPercentRedPaired());
        event.setPercentYellowPaired(log.getPercentYellowPaired());
        event.setPercentGreenPaired(log.getPercentGreenPaired());
        event.setSignalGroupStatistics(log.getSignalGroupStatistics());
        return event;
    }

}
