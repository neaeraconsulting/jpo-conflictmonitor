package us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.List;

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

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    public double getPercentPaired() {
        if (atspmSpatPairs == null) return 0;
        double numPaired = atspmSpatPairs.stream().filter(AtspmSpatPair::isPaired).count();
        double total = atspmSpatPairs.size();
        if (total == 0) return 0;
        return (numPaired  / total) * 100.0;
    }


}
