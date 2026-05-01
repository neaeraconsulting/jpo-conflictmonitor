package us.dot.its.jpo.conflictmonitor.batch;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationAlgorithmFactory;
import us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation.AtspmSpatValidationParameters;

@Getter
@Setter
@ConfigurationProperties
public class ConflictMonitorBatchProcessingProperties {

    private String  atspmSpatValidationAlgorithm;
    private AtspmSpatValidationParameters atspmSpatValidationParameters;
    private AtspmSpatValidationAlgorithmFactory atspmSpatValidationAlgorithmFactory;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    public void setAtspmSpatValidationAlgorithmFactory(AtspmSpatValidationAlgorithmFactory atspmSpatValidationAlgorithmFactory) {
        this.atspmSpatValidationAlgorithmFactory = atspmSpatValidationAlgorithmFactory;
    }

    @Autowired
    public void setAtspmSpatValidationParameters(AtspmSpatValidationParameters atspmSpatValidationParameters) {
        this.atspmSpatValidationParameters = atspmSpatValidationParameters;
        this.atspmSpatValidationAlgorithm = atspmSpatValidationParameters.getAlgorithm();
    }
}
