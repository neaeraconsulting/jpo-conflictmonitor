package us.dot.its.jpo.conflictmonitor.monitor.algorithms.priority_preemption_request;

public interface PriorityPreemptionRequestAlgorithmFactory {
    PriorityPreemptionRequestAlgorithm getAlgorithm(String algorithmName);
}
