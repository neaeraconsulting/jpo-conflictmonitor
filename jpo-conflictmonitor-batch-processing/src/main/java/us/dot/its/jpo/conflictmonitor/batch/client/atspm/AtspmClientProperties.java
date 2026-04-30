package us.dot.its.jpo.conflictmonitor.batch.client.atspm;

import lombok.Data;
import lombok.Generated;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Generated
@Component
@ConfigurationProperties(prefix = "cm.atspm.client")
public class AtspmClientProperties {
    String baseUrl;
    String username;
    String password;
}
