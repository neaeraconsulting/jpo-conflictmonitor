package us.dot.its.jpo.conflictmonitor.batch.services.atspm;

import lombok.Data;
import lombok.Generated;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Generated
@Component
@ConfigurationProperties(prefix = "cm.batch.atspm.spat.validation.client")
public class AtspmClientProperties {
    String baseUrl;
    String username;
    String password;
}
