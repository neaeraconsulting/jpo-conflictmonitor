package us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import org.springframework.data.annotation.AccessType;
import org.springframework.data.mongodb.core.mapping.Document;
import us.dot.its.jpo.conflictmonitor.batch.models.spat.SpatSignalIndication;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
@Document("CmAtspmSpatPairLog")
public class AtspmSpatPairLog {
    /**
     * ATSPM Route ID
     */
    private int routeId;

    /**
     * ATSPM Signal ID
     */
    private String signalId;

    /**
     * Intersection ID from SPAT
     */
    private int intersectionId;

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

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    @AccessType(AccessType.Type.PROPERTY)
    public Double getPercentPaired() {
        if (atspmSpatPairs == null) return null;
        double numPaired = atspmSpatPairs.stream().filter(AtspmSpatPair::isPaired).count();
        double total = atspmSpatPairs.size();
        if (total == 0) return null;
        return (numPaired  / total) * 100.0;
    }


    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    @AccessType(AccessType.Type.PROPERTY)
    public Double getPercentGreenPaired() {
        return percentPaired(SpatSignalIndication.GREEN);
    }


    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    @AccessType(AccessType.Type.PROPERTY)
    public Double getPercentRedPaired() {
        return percentPaired(SpatSignalIndication.RED);
    }


    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    @AccessType(AccessType.Type.PROPERTY)
    public Double getPercentYellowPaired() {
        return percentPaired(SpatSignalIndication.YELLOW);
    }


    private Double percentPaired(final SpatSignalIndication indication) {
        if (atspmSpatPairs == null) return null;
        double numPaired = atspmSpatPairs.stream().filter(AtspmSpatPair::isPaired).filter(pair -> pair.getSpatIndication() == indication).count();
        double total = atspmSpatPairs.stream().filter(pair -> pair.getSpatIndication() == indication).count();
        if (total == 0) return null;
        return (numPaired / total) * 100.0;
    }

    private Double percentPaired(final SpatSignalIndication indication, final int signalGroup) {
        if (atspmSpatPairs == null) return null;
        double numPaired = atspmSpatPairs.stream()
                .filter(pair -> pair.getSpatSignalGroupId() == signalGroup)
                .filter(AtspmSpatPair::isPaired)
                .filter(pair -> pair.getSpatIndication() == indication)
                .count();
        double total = atspmSpatPairs.stream()
                .filter(pair -> pair.getSpatSignalGroupId() == signalGroup)
                .filter(pair -> pair.getSpatIndication() == indication)
                .count();
        if (total == 0) return null;
        return (numPaired / total) * 100.0;
    }

    private long count(final SpatSignalIndication indication, final int signalGroup) {
        if (atspmSpatPairs == null) return 0;
        return atspmSpatPairs.stream()
                .filter(pair -> pair.getSpatSignalGroupId() == signalGroup)
                .filter(pair -> pair.getSpatIndication() == indication)
                .count();
    }

    private Double percentPaired(final int signalGroup) {
        if (atspmSpatPairs == null) return null;
        double numPaired = atspmSpatPairs.stream()
                .filter(pair -> pair.getSpatSignalGroupId() == signalGroup)
                .filter(AtspmSpatPair::isPaired)
                .count();
        double total = atspmSpatPairs.stream()
                .filter(pair -> pair.getSpatSignalGroupId() == signalGroup)
                .count();
        if (total == 0) return null;
        return (numPaired / total) * 100.0;
    }

    private Set<Integer> uniqueSignalGroups() {
        if (atspmSpatPairs == null) return Collections.emptySet();
        return atspmSpatPairs.stream().map(AtspmSpatPair::getSpatSignalGroupId).collect(Collectors.toUnmodifiableSet());
    }

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    @AccessType(AccessType.Type.PROPERTY)
    public AtspmSpatStatistics getSignalGroupStatistics() {
        Set<Integer> uniqueSignalGroups = uniqueSignalGroups();
        AtspmSpatStatistics stats = new AtspmSpatStatistics();
        for (Integer signalGroupId : uniqueSignalGroups) {
            Double green = percentPaired(SpatSignalIndication.GREEN, signalGroupId);
            Double yellow = percentPaired(SpatSignalIndication.YELLOW, signalGroupId);
            Double red = percentPaired(SpatSignalIndication.RED, signalGroupId);
            Double all = percentPaired(signalGroupId);
            long greenCount = count(SpatSignalIndication.GREEN, signalGroupId);
            long yellowCount = count(SpatSignalIndication.YELLOW, signalGroupId);
            long redCount = count(SpatSignalIndication.RED, signalGroupId);
            SignalGroupStatistics sgStats = new SignalGroupStatistics(
                    signalGroupId, green, yellow, red, all, greenCount, yellowCount, redCount);
            stats.put(signalGroupId, sgStats);
        }
        return stats;
    }

}
