package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.ServiceLocatorFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PriorityRequestMetricsAlgorithms {
    @Bean
    public FactoryBean<?> priorityRequestMetricsServiceLocatorFactoryBean() {
        var factoryBean = new ServiceLocatorFactoryBean();
        factoryBean.setServiceLocatorInterface(PriorityRequestMetricsAlgorithmFactory.class);
        return factoryBean;
    }
}
