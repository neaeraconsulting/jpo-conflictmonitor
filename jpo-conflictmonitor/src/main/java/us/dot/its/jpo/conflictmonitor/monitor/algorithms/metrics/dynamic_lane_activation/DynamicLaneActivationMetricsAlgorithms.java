package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.dynamic_lane_activation;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.ServiceLocatorFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DynamicLaneActivationMetricsAlgorithms {
    @Bean
    public FactoryBean<?> dynamicLaneActivationMetricsServiceLocatorFactoryBean() {
        var factoryBean = new ServiceLocatorFactoryBean();
        factoryBean.setServiceLocatorInterface(DynamicLaneActivationMetricsAlgorithmFactory.class);
        return factoryBean;
    }
}
