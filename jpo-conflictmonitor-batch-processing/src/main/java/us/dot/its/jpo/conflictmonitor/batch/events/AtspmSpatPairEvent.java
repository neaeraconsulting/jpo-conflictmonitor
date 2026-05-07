package us.dot.its.jpo.conflictmonitor.batch.events;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import us.dot.its.jpo.conflictmonitor.batch.models.atspm_spat.AtspmSpatPairLog;

/**
 * Events are produced per intersection and per time period if the percentage
 * of paired ATSPM and SPAT events is less than 90%.
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("CmAtspmSpatPairEvent")
public class AtspmSpatPairEvent extends Event {

    public AtspmSpatPairEvent() {
        super("AtspmSpatPair");
    }

    private AtspmSpatPairLog log;
}
