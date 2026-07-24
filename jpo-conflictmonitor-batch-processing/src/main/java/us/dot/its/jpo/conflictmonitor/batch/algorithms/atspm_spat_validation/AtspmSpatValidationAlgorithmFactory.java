package us.dot.its.jpo.conflictmonitor.batch.algorithms.atspm_spat_validation;

public interface AtspmSpatValidationAlgorithmFactory {
    AtspmSpatValidationAlgorithm getAlgorithm(String algorithmName);
}
