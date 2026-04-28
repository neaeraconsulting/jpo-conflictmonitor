package us.dot.its.jpo.conflictmonitor.atspm.client;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AtspmTokenConfig {

    /**
     * Singleton bean to hold the current token
     * @return The token holder, initially empty
     */
    @Bean
    public AtspmToken atspmClientToken() {
        return new AtspmToken();
    }

}
