<<<<<<<< HEAD:jpo-conflictmonitor/src/main/java/us/dot/its/jpo/conflictmonitor/monitor/algorithms/revocable_enabled_lane_alignment/RevocableEnabledLaneAlignmentAlgorithms.java
package us.dot.its.jpo.conflictmonitor.monitor.algorithms.revocable_enabled_lane_alignment;
========
package us.dot.its.jpo.conflictmonitor.monitor.algorithms.metrics.priority_request;
>>>>>>>> neaera/develop:jpo-conflictmonitor/src/main/java/us/dot/its/jpo/conflictmonitor/monitor/algorithms/metrics/priority_request/PriorityRequestMetricsAlgorithms.java

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.config.ServiceLocatorFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
<<<<<<<< HEAD:jpo-conflictmonitor/src/main/java/us/dot/its/jpo/conflictmonitor/monitor/algorithms/revocable_enabled_lane_alignment/RevocableEnabledLaneAlignmentAlgorithms.java
public class RevocableEnabledLaneAlignmentAlgorithms {
    @Bean
    public FactoryBean<?> revocableEnabledLaneAlignmentAlgorithmServiceLocatorFactoryBean() {
        var factoryBean = new ServiceLocatorFactoryBean();
        factoryBean.setServiceLocatorInterface(RevocableEnabledLaneAlignmentAlgorithmFactory.class);
========
public class PriorityRequestMetricsAlgorithms {
    @Bean
    public FactoryBean<?> priorityRequestMetricsServiceLocatorFactoryBean() {
        var factoryBean = new ServiceLocatorFactoryBean();
        factoryBean.setServiceLocatorInterface(PriorityRequestMetricsAlgorithmFactory.class);
>>>>>>>> neaera/develop:jpo-conflictmonitor/src/main/java/us/dot/its/jpo/conflictmonitor/monitor/algorithms/metrics/priority_request/PriorityRequestMetricsAlgorithms.java
        return factoryBean;
    }
}
