package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.temporal.ChronoUnit;
import java.util.Set;

@Data
@Component
@ConfigurationProperties(prefix = "cm.batch.atspm.spat.validation")
public class AtspmSpatValidationParameters {
    private String algorithm;
    private int interval;
    private ChronoUnit intervalUnits;

    @JsonIgnore
    private final static Set<ChronoUnit> validUnits = Set.of(ChronoUnit.HOURS, ChronoUnit.MINUTES, ChronoUnit.SECONDS);

    /**
     * Validates if units are HOURS, MINUTES, or SECONDS
     * @param intervalUnits Units if valid
     */
    public void setIntervalUnits(ChronoUnit intervalUnits) {
        if (!validUnits.contains(intervalUnits)) {
            throw new IllegalArgumentException("Invalid intervalUnits: " + intervalUnits);
        }
        this.intervalUnits = intervalUnits;

    }
}
