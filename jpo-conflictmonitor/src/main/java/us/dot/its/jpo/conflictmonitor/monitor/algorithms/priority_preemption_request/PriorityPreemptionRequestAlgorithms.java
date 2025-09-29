package us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.ServiceLocatorFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PriorityPreemptionRequestAlgorithms {
    @Bean
    public FactoryBean<?> priorityPreemptionRequestServiceLocatorFactoryBean() {
        var factoryBean = new ServiceLocatorFactoryBean();
        factoryBean.setServiceLocatorInterface(PriorityPreemptionRequestAlgorithmFactory.class);
        return factoryBean;
    }
}
