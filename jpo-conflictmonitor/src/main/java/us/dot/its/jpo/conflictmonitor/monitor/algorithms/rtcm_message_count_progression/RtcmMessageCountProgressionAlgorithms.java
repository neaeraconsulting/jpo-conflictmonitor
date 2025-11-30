package us.dot.its.jpo.conflictmonitor.monitor.algorithms.rtcm_message_count_progression;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.ServiceLocatorFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RtcmMessageCountProgressionAlgorithms {
    @Bean
    public FactoryBean<?> rtcmMessageCountProgressionServiceLocatorFactoryBean() {
        var factoryBean = new ServiceLocatorFactoryBean();
        factoryBean.setServiceLocatorInterface(RtcmMessageCountProgressionAlgorithmFactory.class);
        return factoryBean;
    }
}
