package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.ServiceLocatorFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AtspmSpatValidationAlgorithms {
    @Bean
    public FactoryBean<?> atspmSpatValidationServiceLocatorFactoryBean() {
        var factoryBean = new ServiceLocatorFactoryBean();
        factoryBean.setServiceLocatorInterface(AtspmSpatValidationAlgorithmFactory.class);
        return factoryBean;
    }
}
